user ktrun
su - root
groupadd ktgrp
usermod -g ktgrp ktrun
usermod -g ktgrp ktins
chown ktrun /app
chgrp ktgrp /app

sed -i '/^ktrun/s@\/home\/ktrun:\/bin\/bash@\/app\/kt4:\/bin\/csh@' /etc/passwd
sed -i '/^ktins/s@\/home\/ktins:\/bin\/bash@\/app\/kt4:\/bin\/csh@' /etc/passwd
su exit
