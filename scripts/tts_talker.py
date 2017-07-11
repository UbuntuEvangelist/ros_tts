#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import rospy
import time
import re
import logging
import random
import threading
import subprocess
import urllib
from Queue import Queue

from basic_head_api.msg import MakeFaceExpr
from blender_api_msgs.msg import Viseme, SetGesture, EmotionState
from std_msgs.msg import String
from topic_tools.srv import MuxSelect
from dynamic_reconfigure.server import Server

from ttsserver.sound_file import SoundFile
from ttsserver.visemes import BaseVisemes
from ttsserver.client import Client
from ros_tts.srv import *
from ros_tts.cfg import TTSConfig

logger = logging.getLogger('hr.tts.tts_talker')

class TTSTalker:
    def __init__(self):
        self.client = Client()
        self.executor = TTSExecutor()

        self.voices = {}
        self.voices['en'] = rospy.get_param('voice_en', None)
        self.voices['zh'] = rospy.get_param('voice_zh', None)
        for lang in ['en', 'zh']:
            if self.voices[lang] is None:
                logger.error("Voice for {} is not configured".format(lang))
                sys.exit(1)

        self.service = rospy.Service('tts_length', TTSLength, self.tts_length)

        tts_topic = rospy.get_param('tts_topic', 'chatbot_responses')
        rospy.Subscriber(tts_topic, String, self.say)
        rospy.Subscriber(tts_topic+"_en", String, self.say, 'en')
        rospy.Subscriber(tts_topic+"_zh", String, self.say, 'zh')

    def tts_length(self, req):
        text = req.txt
        lang = req.lang
        try:
            if lang in ['en', 'zh']:
                vendor, voice = self.voices[lang].split(':')
                response = self.client.tts(text, vendor=vendor, voice=voice)
                duration = response.get_duration()
                if duration:
                    return TTSLengthResponse(duration)
            else:
                logger.error("Unknown language {}".format(lang))
        except Exception as ex:
            logger.error(ex)
        return TTSLengthResponse(1)

    def say(self, msg, lang=None):
        if not self.enable:
            logger.warn("TTS is not enabled")
            return

        if lang is None:
            lang = rospy.get_param('lang', None)
            if lang is None:
                logger.error("Language is not set")
                return

        text = msg.data

        if lang == 'zh':
            # cut the text by punctuations to avoid lengthy text
            if isinstance(text, str):
                try:
                    text = text.decode('utf-8')
                except Exception as ex:
                    logger.error("Decode error {}, text {}".format(ex, text))
                    return
            pattern = ur'[。|，|！|？|、|.|,|!|?|\n]'
            sentences = re.split(pattern, text)
            for text in sentences:
                text = text.strip()
                if text:
                    self._say(text.encode('utf-8'), lang)
        elif lang == 'en':
            self._say(text, lang)

        logger.info("Finished tts")

    def text_preprocess(self, text):
        if not isinstance(text, unicode):
            text = text.decode('utf-8')

        # St. Patrick's Day => St Patrick's Day
        text = re.sub(r'(?iu)([s]t\. )', 'St ', text)
        # AI => Artificial Intelligence
        #text = re.sub(r'(?iu)(\ba\.?i\.?)\b', 'Artificial Intelligence', text)
        # Hmm => <spurt />
        text = re.sub(r'(?iu)(\bhmm*\b)', '<prosody rate="+100%"><spurt audio="g0001_015">hmm</spurt></prosody>', text)
        # Er => <spurt />
        text = re.sub(r'(?iu)(\berr*\b)', '<prosody rate="+50%"><spurt audio="g0001_017">er</spurt></prosody>', text)
        # Ah
        text = re.sub(r'(?iu)(\bahh*\b)', '<prosody rate="+50%"><spurt audio="g0001_025">ah</spurt></prosody>', text)

        if isinstance(text, unicode):
            text = text.encode('utf-8')

        return text

    def _say(self, text, lang):
        logger.info('Say "{}" in {}'.format(text, lang))
        try:
            text = text.strip().strip(".?!")
            if lang == 'en':
                text = self.text_preprocess(text)
            vendor, voice = self.voices[lang].split(':')
            logger.info("Lang {}, vendor {}, voice {}".format(lang, vendor, voice))
            response = self.client.tts(text, vendor=vendor, voice=voice)
            self.executor.execute(response)
            if self.enable_peer_chatbot:
                curl_url = self.peer_chatbot_url
                text = text.replace("'", "\\'")
                text = urllib.quote(text)
                cmd = r'''curl "{}/say/{}" '''.format(curl_url, text)
                retcode = subprocess.call(cmd, shell=True)
                logger.info("Run command: {}".format(cmd))
                logger.info("Command return code {}".format(retcode))
        except Exception as ex:
            import traceback
            logger.error('TTS error: {}'.format(traceback.format_exc()))

    def reconfig(self, config, level):
        self.enable = config.enable
        self.executor.lipsync_enabled = config.lipsync_enabled
        self.executor.lipsync_blender = config.lipsync_blender
        self.executor.enable_execute_marker(config.execute_marker)
        self.enable_peer_chatbot = config.enable_peer_chatbot
        self.peer_chatbot_url = config.peer_chatbot_url
        return config

class TTSExecutor(object):

    def __init__(self):
        self._locker = threading.RLock()
        self.interrupt = threading.Event()
        self.sound = SoundFile()

        self.lipsync_enabled = rospy.get_param('lipsync', True)
        self.lipsync_blender = rospy.get_param('lipsync_blender', True)

        tts_control = rospy.get_param('tts_control', 'tts_control')
        rospy.Subscriber(tts_control, String, self.tts_control)
        self.speech_active = rospy.Publisher('speech_events', String, queue_size=10)
        self.expr_topic = rospy.Publisher('make_face_expr', MakeFaceExpr, queue_size=0)
        self.vis_topic = rospy.Publisher('/blender_api/queue_viseme', Viseme, queue_size=0)
        self.mux = rospy.ServiceProxy('lips_pau_select', MuxSelect)
        self.blink_publisher = rospy.Publisher('chatbot_blink',String,queue_size=1)

        self.animation_queue = Queue()
        self.animation_runner = AnimationRunner(self.animation_queue)
        self.animation_runner.start()

    def enable_execute_marker(self, enable):
        self.animation_runner.enable_execute_marker(enable)

    def tts_control(self, msg):
        if msg.data == 'shutup':
            logger.info("Shut up!!")
            self.interrupt.set()

    def _startLipSync(self):
        self.speech_active.publish("start")
        if self.lipsync_enabled and not self.lipsync_blender:
            try:
                self.mux("lipsync_pau")
            except Exception as ex:
                logger.error(ex)

    def _stopLipSync(self):
        self.speech_active.publish("stop")
        if self.lipsync_enabled and not self.lipsync_blender:
            try:
                self.mux("head_pau")
            except Exception as ex:
                logger.error(ex)

    def _threadsafe(f):
        def wrap(self, *args, **kwargs):
            self._locker.acquire()
            try:
                return f(self, *args, **kwargs)
            finally:
                self._locker.release()
        return wrap

    @_threadsafe
    def execute(self, response):
        self.interrupt.clear()
        wavfile = os.path.expanduser('~/.hr/tts/tmp.wav')
        success = response.write(wavfile)
        if not success:
            logger.error("No sound file")
            return

        threading.Timer(0.1, self.sound.play, (wavfile,)).start()

        duration = response.get_duration()
        self._startLipSync()
        self.speech_active.publish("duration:%f" % duration)

        phonemes = response.response['phonemes']
        markers = response.response['markers']
        words = response.response['words']
        visemes = response.response['visemes']

        typeorder = {'marker': 1, 'word': 2, 'viseme': 3}
        nodes = markers+words+visemes
        nodes = sorted(nodes, key=lambda x: (x['start'], typeorder[x['type']]))

        # Overwrite visemes during vocal gestures
        in_gesture = False
        vocal_gesture_nodes = []
        for node in nodes:
            if node['type'] == 'marker':
                if node['name'] == 'CPRC_GESTURE_START':
                    in_gesture = True
                if node['name'] == 'CPRC_GESTURE_END':
                    in_gesture = False
            if node['type'] == 'viseme' and in_gesture:
                vocal_gesture_nodes.append(node)
        if len(vocal_gesture_nodes)>0:
            if len(vocal_gesture_nodes) > 1:
                mid = len(vocal_gesture_nodes)/2
                for node in vocal_gesture_nodes[:mid]:
                    node['name'] = 'A-I'
                for node in vocal_gesture_nodes[mid:]:
                    node['name'] = 'M'
            else:
                vocal_gesture_nodes[0]['name'] = 'A-I'

        start = time.time()
        end = start + duration + 1
        stopat = 0

        for i, node in enumerate(nodes):
            while time.time() < end and time.time() < start+node['start']:
                time.sleep(0.001)
            if self.interrupt.is_set():
                logger.info("Interrupt is set")
                if node['type'] != 'phoneme':
                    # we still want to play the remaining phonemes
                    # until it meets 'word' or 'marker'
                    logger.info("Interrupt at {}".format(node))
                    break
            if node['type'] == 'marker':
                logger.info("marker {}".format(node))
                if node['name'].startswith('cp'):
                    continue
                self.animation_queue.put(node)
            elif node['type'] == 'word':
                logger.info("word {}".format(node))
                continue
            elif node['type'] == 'viseme':
                logger.info("viseme {}".format(node))
                self.sendVisime(node)

        elapsed = time.time() - start
        supposed = nodes[-1]['end']
        logger.info("Elapsed {}, nodes duration {}".format(elapsed, supposed))

        if self.interrupt.is_set():
            self.interrupt.clear()
            self.sound.interrupt()
            logger.info("Interrupt flag is cleared")

        self.sendVisime({'name': 'Sil'})
        self._stopLipSync()

    def sendVisime(self, visime):
        if self.lipsync_enabled and self.lipsync_blender and (visime['name'] != 'Sil'):
            #Need to have global shapekey_store class.
            msg = Viseme()
            # Duration should be overlapping
            duration = visime['duration']
            msg.duration.nsecs = duration*1e9*BaseVisemes.visemes_param[visime['name']]['duration']
            msg.name = visime['name']
            msg.magnitude = BaseVisemes.visemes_param[visime['name']]['magnitude']
            msg.rampin = BaseVisemes.visemes_param[visime['name']]['rampin']
            msg.rampout = BaseVisemes.visemes_param[visime['name']]['rampout']
            self.vis_topic.publish(msg)
        if self.lipsync_enabled and not self.lipsync_blender:
            msg = MakeFaceExpr()
            msg.exprname = 'vis_'+visime['name']
            msg.intensity = 1.0
            self.expr_topic.publish(msg)

class AnimationRunner(threading.Thread):

    def __init__(self, queue):
        super(AnimationRunner, self).__init__()
        self.gesture_topic = rospy.Publisher(
            '/blender_api/set_gesture', SetGesture, queue_size=1)
        self.emotion_topic = rospy.Publisher(
            '/blender_api/set_emotion_state', EmotionState, queue_size=1)
        self.queue = queue
        self.daemon = True
        logger.info("Init Animation Runner")
        self.tts_animation_config = rospy.get_param('tts_animation_config', {})
        self.enable = True

    def enable_execute_marker(self, enable):
        self.enable = enable

    def run(self):
        while True:
            try:
                node = self.queue.get()
                if not self.enable:
                    logger.info("Animation runner is not enabled")
                    continue
                logger.info("Get node {}".format(node))
                name = node['name']
                if isinstance(name, unicode):
                    name = name.encode('utf8')
                name = name.lower()
                if ',' in name:
                    name, arg = name.split(',', 1)
                else:
                    arg = ''
                if name in self.tts_animation_config:
                    animation_type, animation_name = self.tts_animation_config[name].split(':')
                    if arg:
                        animation_name = ','.join([animation_name, arg])
                else:
                    logger.error("{} is not configured".format(name))
                    continue
                if animation_type == 'gesture':
                    node['animation'] = animation_name
                    gesture = self.get_gesture(node)
                    self.sendGesture(gesture)
                elif animation_type == 'emotion':
                    node['animation'] = animation_name
                    logger.error(node)
                    emotion = self.get_emotion(node)
                    self.sendEmotion(emotion)
                else:
                    logger.warn("Unrecognized node {}".format(node))
            except Exception as ex:
                import traceback
                logger.error(ex)
                logger.error(traceback.format_exc())

    def get_gesture(self, node):
        gesture = {}
        gesture['start'] = node['start']
        gesture['end'] = node['end']
        gesture['name'] = node['animation'].strip(',')
        return gesture

    def sendGesture(self, gesture):
        msg = SetGesture()
        args = gesture['name'].split(',', 2)
        logger.info(args)
        msg.speed = 1
        msg.magnitude = 1
        if len(args) >= 1:
            msg.name = str(args[0])
        if len(args) >= 2:
            msg.speed = float(args[1])
        if len(args) >= 3:
            msg.magnitude = float(args[2])
        logger.info("Send gesture {}".format(msg))
        self.gesture_topic.publish(msg)

    def get_emotion(self, node):
        emotion = {}
        emotion['name'] = node['animation'].strip(',')
        emotion['start'] = node['start']
        emotion['end'] = node['end']
        return emotion

    def sendEmotion(self, emotion):
        msg = EmotionState()
        args = emotion['name'].split(',', 2)
        logger.info(args)
        msg.magnitude = 1
        msg.duration.secs = 1
        if len(args) >= 1:
            msg.name = str(args[0])
        if len(args) >= 2:
            msg.magnitude = float(args[1])
        if len(args) >= 3:
            msg.duration.secs = float(args[2])
        logger.info("Send emotion {}".format(msg))
        self.emotion_topic.publish(msg)

if __name__ == '__main__':
    rospy.init_node('tts_talker')
    talker = TTSTalker()
    Server(TTSConfig, talker.reconfig)
    rospy.spin()
