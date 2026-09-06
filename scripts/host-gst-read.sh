#!/bin/bash

/d/DATA/dev/source/gstreamer/_runtime/1.0/msvc_x86_64/bin/gst-launch-1.0.exe \
    udpsrc port=5000 caps="application/x-rtp,encoding-name=JPEG,payload=26" ! rtpjitterbuffer ! rtpjpegdepay ! jpegdec ! videoconvert ! autovideosink