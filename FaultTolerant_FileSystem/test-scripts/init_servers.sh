
base_path=/home/rv/junk/fusepy
meta_server="51234"
data_server="51235 51236 51237 51238" 
 
# "cd ${base_path}; python metaserver.py ${meta_server}"
#gnome-terminal -e "cd ${base_path}; python dataserver.py ${data_server}"

cd $base_path
gnome-terminal -e "python metaserver.py ${meta_server}" &
gnome-terminal -e "python dataserver.py ${data_server}" &
sleep 1



