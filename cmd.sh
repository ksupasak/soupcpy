

adb exec-out screenrecord --output-format=h264 - | ffplay -f h264 -


adb exec-out screenrecord --size 1280x720 --bit-rate 200000 --output-format=h264 - \
| ffplay -fflags nobuffer -flags low_delay -framedrop -probesize 32 -sync video -vf fps=15 -i -


adb exec-out screenrecord --size 1280x720 --bit-rate 200000 --output-format=h264 - | ffplay -f h264 -


adb exec-out screenrecord --size 600x360 --bit-rate 200000 --output-format=h264 - | ffplay -f h264 -


adb exec-out screenrecord --size 600x360  --output-format=h264 - | ffplay -f h264 -


adb exec-out screenrecord  --output-format=h264 - | ffplay -f h264 -


adb exec-out screenrecord   | ffplay 
localhost:3000/stream#!action=stream&udid=R5CY31QK16N&player=webcodecs&ws=ws%3A%2F%2Flocalhost%3A4567%2Fstream%3Faction%3Dproxy-adb%26remote%3Dtcp%253A8886%26udid%3DR5CY31QK16Nhttp://localhost:4567/#!action=stream&udid=R5CY31QK16N&player=webcodecs&ws=ws%3A%2F%2Flocalhost%3A4567%2Fstream%3Faction%3Dproxy-adb%26remote%3Dtcp%253A8886%26udid%3DR5CY31QK16N

http://localhost:3000/stream#!action=stream&udid=192.168.1.66:5555&player=webcodecs&ws=ws%3A%2F%2Flocalhost%3A3000%2Fws%3Faction%3Dproxy-adb%26remote%3Dtcp%253A8886%26udid%3D192.168.1.66:5555

http://127.0.0.1:3000/stream#!action=stream&udid=192.168.1.66%3A5555&player=webcodecs&ws=ws%3A%2F%2F127.0.0.1%3A3000%2Fws%3Faction%3Dproxy-adb%26remote%3Dtcp-8886%26udid%3D192.168.1.66%253A5555

http://localhost:3000/stream#!action=stream&udid=R5CY31QK16N&player=webcodecs&ws=ws%3A%2F%2Flocalhost%3A3000%2Fws/receiver%3Faction%3Dproxy-adb%26remote%3Dtcp%253A8886%26udid%3DR5CY31QK16N
http://localhost:3000/stream#!action=stream&udid=R5CY31QK16N&player=webcodecs&ws=ws%3A%2F%2Flocalhost%3A3000%2Fws%3Faction%3Dproxy-adb%26remote%3Dtcp%253A8886%26udid%3DR5CY31QK16N

http://localhost:3000/stream?action=stream&udid=R5CY31QK16N&player=webcodecs&ws=ws%3A%2F%2Flocalhost%3A4567%2Fstream%3Faction%3Dproxy-adb%26remote%3Dtcp%253A8886%26udid%3DR5CY31QK16N
http://localhost:3000/stream/#!action=stream&udid=R5CY31QK16N&player=webcodecs&ws=ws%3A%2F%2Flocalhost%3A3000%2Fws%3Faction%3Dproxy-adb%26remote%3Dtcp%253A8886%26udid%3DR5CY31QK16N

scrcpy --max-size 720 --video-bit-rate 200K --max-fps 15


scrcpy --no-playback --v4l2-sink=/dev/video2 \
       --max-size 720 --max-fps 15 --video-bit-rate 200K


scrcpy --no-playback --record=/tmp/scrcpy.mkv --record-format=mkv \
--max-size 720 --max-fps 15 --video-bit-rate 200K & 

ffmpeg -re -i /tmp/scrcpy.mkv -c copy out.mkv∂


ws://127.0.0.1:8000/?action=proxy-adb&remote=tcp%3A8886&udid=R5CY31QK16N


scrcpy --no-playback --record=/tmp/scrcpy.mkv --record-format=mkv \
       --max-size 720 --max-fps 15 --video-bit-rate 200K &


       ffmpeg -re -i /tmp/scrcpy.mkv -c copy out.mkv


ffplay -fflags nobuffer -flags low_delay -framedrop \
       -probesize 32 -analyzeduration 0 -sync video /tmp/scrcpy.mkv


       scrcpy --new-display=1920x1080 --start-app=org.videolan.vlc