user ktrun
su - root
sed -i '/^ktrun/s@\/home\/ktrun:\/bin\/bash@\/app\/kt4:\/bin\/csh@' /home/ktrun/passwd
sed -i '/^ktins/s@\/home\/ktins:\/bin\/bash@\/app\/kt4:\/bin\/csh@' /home/ktrun/passwd
su exit
