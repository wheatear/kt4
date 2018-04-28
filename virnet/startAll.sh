#!/usr/bin/sh
nohup ./virnetem.py hssnsn_7777.cfg             >../log/virnet.log  2>&1      &
nohup ./virnetem.py pccnsn_7080.cfg            >../log/virnet.log  2>&1      &
nohup ./virnetem.py centrex_scom_14387.cfg      >../log/virnet.log  2>&1      &
nohup ./virnetem.py edsmp_vgop_9080.cfg           >../log/virnet.log  2>&1      &
nohup ./virnetem.py epshss_8001.cfg             >../log/virnet.log  2>&1      &
nohup ./virnetem.py femto_pcchw_8180.cfg         >../log/virnet.log  2>&1      &
nohup ./virnetem.py hsshwCentrex_volte_enum_8080.cfg   >../log/virnet.log  2>&1      &
nohup ./virnetem.py ms_8787.cfg                  >../log/virnet.log  2>&1      &
nohup ./virnetem.py hsshw_8002.cfg   >../log/virnet.log  2>&1      &
