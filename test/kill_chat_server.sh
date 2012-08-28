sudo kill -9 $(ps aux | grep chat_server | head -n 1 | awk '{print $2}')
