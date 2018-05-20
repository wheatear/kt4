#! /bin/perl -w

use IO::Socket::INET;
use Time::HiRes qw(gettimeofday);
use POSIX qw(strftime);
use IO::Select;
use POSIX ":signal_h"; 
use Thread;
use strict;


our $LOG_NAME="../log/in_daemon.log";


sub print_hex{
    my($msg) = @_;
    my @bits = unpack("C*", $msg);
    my $i = 0;
    my $bit;
    foreach $bit (@bits){
        printf("%02x ", $bit);
        $i++;
        if ($i == 16){
            $i=0;
            print "\n";
        }
    }
    print "\n";
}

sub print_log{
    my ($msg) = @_;
    
    open(LOG,">>$LOG_NAME") or die(" Open $LOG_NAME$!");
    
    if (-s "$LOG_NAME" > 1000000000) {
        close LOG;
        `gzip $LOG_NAME`;
        `mv -f $LOG_NAME.gz $LOG_NAME.old.gz`;
        open(LOG,">>$LOG_NAME") or die(" Open $LOG_NAME$!");
    }
    
    my $strtime = strftime "%Y-%m-%d %H:%M:%S", localtime;
    my $line=$$."[$strtime] $msg\n";
    
    print $line;
    print LOG $line;
    close LOG;  
}

#DISP IUSER USERINFO
sub ack_disp_iuser_userinfo{
    my ($request) = @_;
    my $msisdn;
    if ($request =~m/MSISDN=([\d]+),/){
        $msisdn = $1;
    }        
    print_log("$request\n"); 
    my @attr;
    if ($request =~m/ATTR=([\w&]+)/){
        @attr = split /&/, $1;
    }else{
        push(@attr,"SERVICEAREA");
        push(@attr,"MSTYPE");
        push(@attr,"MSISDN");
        push(@attr,"GROUPNO");
        push(@attr,"PKGFEETYPE");
        push(@attr,"MONETPKG");
        push(@attr,"MMSPKG");
        push(@attr,"GPRSPKG");
        push(@attr,"NEXTMONGROUPNO");
        push(@attr,"NEXTMONPKGFEETYPE");
        push(@attr,"NEXTMONMONETPKG");
        push(@attr,"NEXTMONMMSPKG");
        push(@attr,"NEXTMONGPRSPKG");
        push(@attr,"PUBLICUSERID");
        push(@attr,"SUBSTATE");
        push(@attr,"ACCOUNTSTATE");
        push(@attr,"USESTATE");
        push(@attr,"SERVICESTART");
        push(@attr,"SERVICESTOP");
        push(@attr,"CALLSERVICESTOP");
        push(@attr,"ACCOUNTSTOP");
        push(@attr,"LASTPAYRENTDATE");
        push(@attr,"LASTPAYKEEPNUMDATE");
        push(@attr,"REMITDATEACCT1");
        push(@attr,"GROUPTYPE");
        push(@attr,"CALLEDDISCHARGFLAG");
        push(@attr,"CALLINGSCREENFLAG");
        push(@attr,"CALLMOBILEFLAG");
        push(@attr,"CALLTABLEFLAG");
        push(@attr,"CALLEDSCREENFLAG");
        push(@attr,"MCOUNTTOTAL");
        push(@attr,"MLEFTACCOUNT");
        push(@attr,"SRVSTARTDISINDEX");
        push(@attr,"MSDISINDEX");
        push(@attr,"MSDISCOUNT");
        push(@attr,"CREDITLIMIT");
        push(@attr,"ACCOUNTLEFT");
        push(@attr,"MONREMITFROMACCT1");
        push(@attr,"ACCOUNTLEFT1");
        push(@attr,"ACCOUNTRENT");
        push(@attr,"RECORDNO");
        push(@attr,"MMSLEFT");
        push(@attr,"GPRSFLOWLEFT");
        push(@attr,"ALLTIMELEFT");
        push(@attr,"CALLINGTIMELEFT");
        push(@attr,"CALLEDTIMELEFT");
        push(@attr,"INTERTIMELEFT");
        push(@attr,"OUTTIMELEFT");
        push(@attr,"TOLLTIMELEFT");
        push(@attr,"IPTOLLTIMELEFT");
        push(@attr,"OWEPKGRENTAMT");
        push(@attr,"OWEMMSRENTAMT");
        push(@attr,"OWEDATARENTAMT");
        push(@attr,"OWEGPRSRENTAMT");
        push(@attr,"OWEASRENTAMT");
        push(@attr,"OWEOTHERAMT");
        push(@attr,"WAPCUMULATETIME");
        push(@attr,"CUMULATEAMT");
        push(@attr,"CUMULATEBAKAMT");
        push(@attr,"FAULTTIMES");
        push(@attr,"RECHARGECOUNT");
        push(@attr,"RECHARGEAMOUNT");
        push(@attr,"MMSCOUNT");
        push(@attr,"CALLINGCUMAMT");
        push(@attr,"CALLEDCUMAMT");
        push(@attr,"MULTISERVICEFLAG");
        push(@attr,"ASFLAG");
        push(@attr,"ASRENTFLAG");
        push(@attr,"HLRASFLAG");
        push(@attr,"CURGRPACTIVEDATE");
        push(@attr,"CURPKGACTIVEDATE");
        push(@attr,"REQUIRELOSTDATE");
        push(@attr,"REQUIRESTOPDATE");
        push(@attr,"CALLSERVICESTOPBAK");
        push(@attr,"ACCOUNTSTOPBAK");
        push(@attr,"NEXTMONASFLAG");
        push(@attr,"GIFTACCTLEFT");
        push(@attr,"GIFTACCTSTOPDATE");
        push(@attr,"CUMGIFTAMT");
        push(@attr,"LASTGIFTDATE");
        push(@attr,"CTDGROUPINDEX");
        push(@attr,"DURATIONINDEX");
        push(@attr,"TIMEINDEX");
        push(@attr,"CTDCTRLFLAG");
        push(@attr,"FUNDACC3LEFT");
        push(@attr,"FUNDACC3STOPDATE");
        push(@attr,"FUNDACC4LEFT");
        push(@attr,"FUNDACC4STOPDATE");
        push(@attr,"FUNDACC5LEFT");
        push(@attr,"FUNDACC5STOPDATE");
        push(@attr,"FUNDACC6LEFT");
        push(@attr,"FUNDACC6STOPDATE");
        push(@attr,"FUNDACC7LEFT");
        push(@attr,"FUNDACC7STOPDATE");
        push(@attr,"FUNDACC8LEFT");
        push(@attr,"FUNDACC8STOPDATE");
        push(@attr,"FUNDACC9LEFT");
        push(@attr,"FUNDACC9STOPDATE");
        push(@attr,"FUNDACC10LEFT");
        push(@attr,"FUNDACC10STOPDATE");
        push(@attr,"CURMONPKG5");
        push(@attr,"NEXTMONPKG5");
        push(@attr,"OWEPKG5AMT");
        push(@attr,"CURMONPKG6");
        push(@attr,"NEXTMONPKG6");
        push(@attr,"OWEPKG6AMT");
        push(@attr,"CURMONPKG7");
        push(@attr,"NEXTMONPKG7");
        push(@attr,"OWEPKG7AMT");
        push(@attr,"CURMONPKG8");
        push(@attr,"NEXTMONPKG8");
        push(@attr,"OWEPKG8AMT");
        push(@attr,"CURMONPKG9");
        push(@attr,"NEXTMONPKG9");
        push(@attr,"OWEPKG9AMT");
        push(@attr,"CURMONPKG10");
        push(@attr,"NEXTMONPKG10");
        push(@attr,"OWEPKG10AMT");
        push(@attr,"OWEBILL2AMT");
        push(@attr,"OWEBILL3AMT");
        push(@attr,"OWEBILL4AMT");
        push(@attr,"OWEBILL5AMT");
        push(@attr,"GIFTRESLEFT11");
        push(@attr,"GIFTRESLEFT12");
        push(@attr,"GIFTRESLEFT13");
        push(@attr,"GIFTRESLEFT14");
        push(@attr,"GIFTRESLEFT15");
        push(@attr,"GIFTRESLEFT16");
        push(@attr,"GIFTRESLEFT17");
        push(@attr,"GIFTRESLEFT18");
        push(@attr,"GIFTRESLEFT19");
        push(@attr,"GIFTRESLEFT20");
        push(@attr,"SPECRES1LEFT");
        push(@attr,"SPECRES1STOPDATE");
        push(@attr,"SPECRES2LEFT");
        push(@attr,"SPECRES2STOPDATE");
        push(@attr,"SPECRES3LEFT");
        push(@attr,"SPECRES3STOPDATE");
        push(@attr,"SPECRES4LEFT");
        push(@attr,"SPECRES4STOPDATE");
        push(@attr,"SPECRES5LEFT");
        push(@attr,"SPECRES5STOPDATE");
        push(@attr,"SPECRES6LEFT");
        push(@attr,"SPECRES6STOPDATE");
        push(@attr,"SPECRES7LEFT");
        push(@attr,"SPECRES7STOPDATE");
        push(@attr,"SPECRES8LEFT");
        push(@attr,"SPECRES8STOPDATE");
        push(@attr,"SPECRES9LEFT");
        push(@attr,"SPECRES9STOPDATE");
        push(@attr,"SPECRES10LEFT");
        push(@attr,"SPECRES10STOPDATE");
        push(@attr,"CONSUMEACC6");
        push(@attr,"CONSUMEACC7");
        push(@attr,"CONSUMEACC8");
        push(@attr,"CONSUMEACC9");
        push(@attr,"CONSUMEACC10");
        push(@attr,"CONSUMEACC11");
        push(@attr,"CONSUMEACC12");
        push(@attr,"CONSUMEACC13");
        push(@attr,"CONSUMEACC14");
        push(@attr,"CONSUMEACC15");
        push(@attr,"PKGRENTFLAG");
        push(@attr,"MULTISRVFLAGSTATE");
        push(@attr,"ASFLAGSTATE");
        push(@attr,"DATEFIELD1");
        push(@attr,"STRFIELD1");
        push(@attr,"STRFIELD2");
        push(@attr,"STRFIELD3");
        push(@attr,"INTFIELD7");
        push(@attr,"CALLINGINTERTIME");
        push(@attr,"CALLINGOUTTIME");
        push(@attr,"CALLEDINTERTIME");
        push(@attr,"CALLEDOUTTIME");
        push(@attr,"UNAME");
        push(@attr,"USERSEX");
        push(@attr,"IDENTITYCARD");
        push(@attr,"CARDADDRESS");
        push(@attr,"HOMEADDRESS");
        push(@attr,"POSTNO");
        push(@attr,"CALLINGNAME");
        push(@attr,"MINCONSUMELEVEL");
        push(@attr,"USERMSG");
        push(@attr,"GRPMULTISRVFLAG");
        push(@attr,"OWEBORROWAMT");
    }

    my ($att_info,$ret_info,$name);
    for $name (@attr){
        if ($name eq "SERVICEAREA")
        {$att_info = $att_info."SERVICEAREA&";$ret_info = $ret_info."010010&";}
        elsif ($name eq "MSTYPE")
        {$att_info = $att_info."MSTYPE&";$ret_info = $ret_info."5&";}
        elsif ($name eq "MSISDN")
        {$att_info = $att_info."MSISDN&";$ret_info = $ret_info.$msisdn."&";}
        elsif ($name eq "GROUPNO")
        {$att_info = $att_info."GROUPNO&";$ret_info = $ret_info."10|iuser神州行&";}
        elsif ($name eq "PKGFEETYPE")
        {$att_info = $att_info."PKGFEETYPE&";$ret_info = $ret_info."0|主话音套餐&";}
        elsif ($name eq "MONETPKG")
        {$att_info = $att_info."MONETPKG&";$ret_info = $ret_info."-1|无动感地带套餐&";}
        elsif ($name eq "MMSPKG")
        {$att_info = $att_info."MMSPKG&";$ret_info = $ret_info."-1|无彩信套餐&";}
        elsif ($name eq "GPRSPKG")
        {$att_info = $att_info."GPRSPKG&";$ret_info = $ret_info."32011|GPRS5元套餐&";}
        elsif ($name eq "NEXTMONGROUPNO")
        {$att_info = $att_info."NEXTMONGROUPNO&";$ret_info = $ret_info."10|iuser神州行&";}
        elsif ($name eq "NEXTMONPKGFEETYPE")
        {$att_info = $att_info."NEXTMONPKGFEETYPE&";$ret_info = $ret_info."0|主话音套餐&";}
        elsif ($name eq "NEXTMONMONETPKG")
        {$att_info = $att_info."NEXTMONMONETPKG&";$ret_info = $ret_info."-1|无动感地带下月套餐&";}
        elsif ($name eq "NEXTMONMMSPKG")
        {$att_info = $att_info."NEXTMONMMSPKG&";$ret_info = $ret_info."-1|无彩信下月套餐&";}
        elsif ($name eq "NEXTMONGPRSPKG")
        {$att_info = $att_info."NEXTMONGPRSPKG&";$ret_info = $ret_info."32011|GPRS5元套餐&";}
        elsif ($name eq "PUBLICUSERID")
        {$att_info = $att_info."PUBLICUSERID&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SUBSTATE")
        {$att_info = $att_info."SUBSTATE&";$ret_info = $ret_info."1&";}
        elsif ($name eq "ACCOUNTSTATE")
        {$att_info = $att_info."ACCOUNTSTATE&";$ret_info = $ret_info."1&";}
        elsif ($name eq "USESTATE")
        {$att_info = $att_info."USESTATE&";$ret_info = $ret_info."1&";}
        elsif ($name eq "SERVICESTART")
        {$att_info = $att_info."SERVICESTART&";$ret_info = $ret_info."2009-06-23&";}
        elsif ($name eq "SERVICESTOP")
        {$att_info = $att_info."SERVICESTOP&";$ret_info = $ret_info."2039-05-06&";}
        elsif ($name eq "CALLSERVICESTOP")
        {$att_info = $att_info."CALLSERVICESTOP&";$ret_info = $ret_info."2013-01-01&";}
        elsif ($name eq "ACCOUNTSTOP")
        {$att_info = $att_info."ACCOUNTSTOP&";$ret_info = $ret_info."2013-01-01&";}
        elsif ($name eq "LASTPAYRENTDATE")
        {$att_info = $att_info."LASTPAYRENTDATE&";$ret_info = $ret_info."2011-08-01&";}
        elsif ($name eq "LASTPAYKEEPNUMDATE")
        {$att_info = $att_info."LASTPAYKEEPNUMDATE&";$ret_info = $ret_info."2011-01-01&";}
        elsif ($name eq "REMITDATEACCT1")
        {$att_info = $att_info."REMITDATEACCT1&";$ret_info = $ret_info."2009-01-01&";}
        elsif ($name eq "GROUPTYPE")
        {$att_info = $att_info."GROUPTYPE&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLEDDISCHARGFLAG")
        {$att_info = $att_info."CALLEDDISCHARGFLAG&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLINGSCREENFLAG")
        {$att_info = $att_info."CALLINGSCREENFLAG&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLMOBILEFLAG")
        {$att_info = $att_info."CALLMOBILEFLAG&";$ret_info = $ret_info."1&";}
        elsif ($name eq "CALLTABLEFLAG")
        {$att_info = $att_info."CALLTABLEFLAG&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLEDSCREENFLAG")
        {$att_info = $att_info."CALLEDSCREENFLAG&";$ret_info = $ret_info."0&";}
        elsif ($name eq "MCOUNTTOTAL")
        {$att_info = $att_info."MCOUNTTOTAL&";$ret_info = $ret_info."500000&";}
        elsif ($name eq "MLEFTACCOUNT")
        {$att_info = $att_info."MLEFTACCOUNT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SRVSTARTDISINDEX")
        {$att_info = $att_info."SRVSTARTDISINDEX&";$ret_info = $ret_info."0&";}
        elsif ($name eq "MSDISINDEX")
        {$att_info = $att_info."MSDISINDEX&";$ret_info = $ret_info."0&";}
        elsif ($name eq "MSDISCOUNT")
        {$att_info = $att_info."MSDISCOUNT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CREDITLIMIT")
        {$att_info = $att_info."CREDITLIMIT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "ACCOUNTLEFT")
        {$att_info = $att_info."ACCOUNTLEFT&";$ret_info = $ret_info."230&";}
        elsif ($name eq "MONREMITFROMACCT1")
        {$att_info = $att_info."MONREMITFROMACCT1&";$ret_info = $ret_info."0&";}
        elsif ($name eq "ACCOUNTLEFT1")
        {$att_info = $att_info."ACCOUNTLEFT1&";$ret_info = $ret_info."0&";}
        elsif ($name eq "ACCOUNTRENT")
        {$att_info = $att_info."ACCOUNTRENT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "RECORDNO")
        {$att_info = $att_info."RECORDNO&";$ret_info = $ret_info."0&";}
        elsif ($name eq "MMSLEFT")
        {$att_info = $att_info."MMSLEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GPRSFLOWLEFT")
        {$att_info = $att_info."GPRSFLOWLEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "ALLTIMELEFT")
        {$att_info = $att_info."ALLTIMELEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLINGTIMELEFT")
        {$att_info = $att_info."CALLINGTIMELEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLEDTIMELEFT")
        {$att_info = $att_info."CALLEDTIMELEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "INTERTIMELEFT")
        {$att_info = $att_info."INTERTIMELEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OUTTIMELEFT")
        {$att_info = $att_info."OUTTIMELEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "TOLLTIMELEFT")
        {$att_info = $att_info."TOLLTIMELEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "IPTOLLTIMELEFT")
        {$att_info = $att_info."IPTOLLTIMELEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEPKGRENTAMT")
        {$att_info = $att_info."OWEPKGRENTAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEMMSRENTAMT")
        {$att_info = $att_info."OWEMMSRENTAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEDATARENTAMT")
        {$att_info = $att_info."OWEDATARENTAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEGPRSRENTAMT")
        {$att_info = $att_info."OWEGPRSRENTAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEASRENTAMT")
        {$att_info = $att_info."OWEASRENTAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEOTHERAMT")
        {$att_info = $att_info."OWEOTHERAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "WAPCUMULATETIME")
        {$att_info = $att_info."WAPCUMULATETIME&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CUMULATEAMT")
        {$att_info = $att_info."CUMULATEAMT&";$ret_info = $ret_info."34&";}
        elsif ($name eq "CUMULATEBAKAMT")
        {$att_info = $att_info."CUMULATEBAKAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "FAULTTIMES")
        {$att_info = $att_info."FAULTTIMES&";$ret_info = $ret_info."0&";}
        elsif ($name eq "RECHARGECOUNT")
        {$att_info = $att_info."RECHARGECOUNT&";$ret_info = $ret_info."22&";}
        elsif ($name eq "RECHARGEAMOUNT")
        {$att_info = $att_info."RECHARGEAMOUNT&";$ret_info = $ret_info."98000&";}
        elsif ($name eq "MMSCOUNT")
        {$att_info = $att_info."MMSCOUNT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLINGCUMAMT")
        {$att_info = $att_info."CALLINGCUMAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLEDCUMAMT")
        {$att_info = $att_info."CALLEDCUMAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "MULTISERVICEFLAG")
        {$att_info = $att_info."MULTISERVICEFLAG&";$ret_info = $ret_info."0000f0000000001001001000000000000100&";}
        elsif ($name eq "ASFLAG")
        {$att_info = $att_info."ASFLAG&";$ret_info = $ret_info."000000001000000000000000&";}
        elsif ($name eq "ASRENTFLAG")
        {$att_info = $att_info."ASRENTFLAG&";$ret_info = $ret_info."000000001000000000000000&";}
        elsif ($name eq "HLRASFLAG")
        {$att_info = $att_info."HLRASFLAG&";$ret_info = $ret_info."000000001000000000000000&";}
        elsif ($name eq "CURGRPACTIVEDATE")
        {$att_info = $att_info."CURGRPACTIVEDATE&";$ret_info = $ret_info."2009-10-01&";}
        elsif ($name eq "CURPKGACTIVEDATE")
        {$att_info = $att_info."CURPKGACTIVEDATE&";$ret_info = $ret_info."2009-10-01&";}
        elsif ($name eq "REQUIRELOSTDATE")
        {$att_info = $att_info."REQUIRELOSTDATE&";$ret_info = $ret_info."2010-09-21&";}
        elsif ($name eq "REQUIRESTOPDATE")
        {$att_info = $att_info."REQUIRESTOPDATE&";$ret_info = $ret_info."1990-01-01&";}
        elsif ($name eq "CALLSERVICESTOPBAK")
        {$att_info = $att_info."CALLSERVICESTOPBAK&";$ret_info = $ret_info."2011-08-03&";}
        elsif ($name eq "ACCOUNTSTOPBAK")
        {$att_info = $att_info."ACCOUNTSTOPBAK&";$ret_info = $ret_info."2011-11-03&";}
        elsif ($name eq "NEXTMONASFLAG")
        {$att_info = $att_info."NEXTMONASFLAG&";$ret_info = $ret_info."------------------------&";}
        elsif ($name eq "GIFTACCTLEFT")
        {$att_info = $att_info."GIFTACCTLEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTACCTSTOPDATE")
        {$att_info = $att_info."GIFTACCTSTOPDATE&";$ret_info = $ret_info."2030-01-01&";}
        elsif ($name eq "CUMGIFTAMT")
        {$att_info = $att_info."CUMGIFTAMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "LASTGIFTDATE")
        {$att_info = $att_info."LASTGIFTDATE&";$ret_info = $ret_info."1990-01-01&";}
        elsif ($name eq "CTDGROUPINDEX")
        {$att_info = $att_info."CTDGROUPINDEX&";$ret_info = $ret_info."0|未确定归属分区&";}
        elsif ($name eq "DURATIONINDEX")
        {$att_info = $att_info."DURATIONINDEX&";$ret_info = $ret_info."0&";}
        elsif ($name eq "TIMEINDEX")
        {$att_info = $att_info."TIMEINDEX&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CTDCTRLFLAG")
        {$att_info = $att_info."CTDCTRLFLAG&";$ret_info = $ret_info."00000000&";}
        elsif ($name eq "FUNDACC3LEFT")
        {$att_info = $att_info."FUNDACC3LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "FUNDACC3STOPDATE")
        {$att_info = $att_info."FUNDACC3STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "FUNDACC4LEFT")
        {$att_info = $att_info."FUNDACC4LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "FUNDACC4STOPDATE")
        {$att_info = $att_info."FUNDACC4STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "FUNDACC5LEFT")
        {$att_info = $att_info."FUNDACC5LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "FUNDACC5STOPDATE")
        {$att_info = $att_info."FUNDACC5STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "FUNDACC6LEFT")
        {$att_info = $att_info."FUNDACC6LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "FUNDACC6STOPDATE")
        {$att_info = $att_info."FUNDACC6STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "FUNDACC7LEFT")
        {$att_info = $att_info."FUNDACC7LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "FUNDACC7STOPDATE")
        {$att_info = $att_info."FUNDACC7STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "FUNDACC8LEFT")
        {$att_info = $att_info."FUNDACC8LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "FUNDACC8STOPDATE")
        {$att_info = $att_info."FUNDACC8STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "FUNDACC9LEFT")
        {$att_info = $att_info."FUNDACC9LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "FUNDACC9STOPDATE")
        {$att_info = $att_info."FUNDACC9STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "FUNDACC10LEFT")
        {$att_info = $att_info."FUNDACC10LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "FUNDACC10STOPDATE")
        {$att_info = $att_info."FUNDACC10STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "CURMONPKG5")
        {$att_info = $att_info."CURMONPKG5&";$ret_info = $ret_info."-1|无当前月第5种套餐&";}
        elsif ($name eq "NEXTMONPKG5")
        {$att_info = $att_info."NEXTMONPKG5&";$ret_info = $ret_info."-1|无下月第5种套餐&";}
        elsif ($name eq "OWEPKG5AMT")
        {$att_info = $att_info."OWEPKG5AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CURMONPKG6")
        {$att_info = $att_info."CURMONPKG6&";$ret_info = $ret_info."52001|长漫亲情省&";}
        elsif ($name eq "NEXTMONPKG6")
        {$att_info = $att_info."NEXTMONPKG6&";$ret_info = $ret_info."52001|长漫亲情省&";}
        elsif ($name eq "OWEPKG6AMT")
        {$att_info = $att_info."OWEPKG6AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CURMONPKG7")
        {$att_info = $att_info."CURMONPKG7&";$ret_info = $ret_info."-1|无当前月第7种套餐&";}
        elsif ($name eq "NEXTMONPKG7")
        {$att_info = $att_info."NEXTMONPKG7&";$ret_info = $ret_info."-1|无下月第7种套餐&";}
        elsif ($name eq "OWEPKG7AMT")
        {$att_info = $att_info."OWEPKG7AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CURMONPKG8")
        {$att_info = $att_info."CURMONPKG8&";$ret_info = $ret_info."-1|无当前月第8种套餐&";}
        elsif ($name eq "NEXTMONPKG8")
        {$att_info = $att_info."NEXTMONPKG8&";$ret_info = $ret_info."-1|无下月第8种套餐&";}
        elsif ($name eq "OWEPKG8AMT")
        {$att_info = $att_info."OWEPKG8AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CURMONPKG9")
        {$att_info = $att_info."CURMONPKG9&";$ret_info = $ret_info."-1|无当前月第9种套餐&";}
        elsif ($name eq "NEXTMONPKG9")
        {$att_info = $att_info."NEXTMONPKG9&";$ret_info = $ret_info."-1|无下月第9种套餐&";}
        elsif ($name eq "OWEPKG9AMT")
        {$att_info = $att_info."OWEPKG9AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CURMONPKG10")
        {$att_info = $att_info."CURMONPKG10&";$ret_info = $ret_info."-1|无当前月第10种套餐&";}
        elsif ($name eq "NEXTMONPKG10")
        {$att_info = $att_info."NEXTMONPKG10&";$ret_info = $ret_info."-1|无下月第10种套餐&";}
        elsif ($name eq "OWEPKG10AMT")
        {$att_info = $att_info."OWEPKG10AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEBILL2AMT")
        {$att_info = $att_info."OWEBILL2AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEBILL3AMT")
        {$att_info = $att_info."OWEBILL3AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEBILL4AMT")
        {$att_info = $att_info."OWEBILL4AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "OWEBILL5AMT")
        {$att_info = $att_info."OWEBILL5AMT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT11")
        {$att_info = $att_info."GIFTRESLEFT11&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT12")
        {$att_info = $att_info."GIFTRESLEFT12&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT13")
        {$att_info = $att_info."GIFTRESLEFT13&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT14")
        {$att_info = $att_info."GIFTRESLEFT14&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT15")
        {$att_info = $att_info."GIFTRESLEFT15&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT16")
        {$att_info = $att_info."GIFTRESLEFT16&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT17")
        {$att_info = $att_info."GIFTRESLEFT17&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT18")
        {$att_info = $att_info."GIFTRESLEFT18&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT19")
        {$att_info = $att_info."GIFTRESLEFT19&";$ret_info = $ret_info."0&";}
        elsif ($name eq "GIFTRESLEFT20")
        {$att_info = $att_info."GIFTRESLEFT20&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES1LEFT")
        {$att_info = $att_info."SPECRES1LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES1STOPDATE")
        {$att_info = $att_info."SPECRES1STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "SPECRES2LEFT")
        {$att_info = $att_info."SPECRES2LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES2STOPDATE")
        {$att_info = $att_info."MSTYPE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "SPECRES3LEFT")
        {$att_info = $att_info."SPECRES3LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES3STOPDATE")
        {$att_info = $att_info."SPECRES3STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "SPECRES4LEFT")
        {$att_info = $att_info."SPECRES4LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES4STOPDATE")
        {$att_info = $att_info."SPECRES4STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "SPECRES5LEFT")
        {$att_info = $att_info."SPECRES5LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES5STOPDATE")
        {$att_info = $att_info."SPECRES5STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "SPECRES6LEFT")
        {$att_info = $att_info."SPECRES6LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES6STOPDATE")
        {$att_info = $att_info."SPECRES6STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "SPECRES7LEFT")
        {$att_info = $att_info."SPECRES7LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES7STOPDATE")
        {$att_info = $att_info."SPECRES7STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "SPECRES8LEFT")
        {$att_info = $att_info."SPECRES8LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES8STOPDATE")
        {$att_info = $att_info."SPECRES8STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "SPECRES9LEFT")
        {$att_info = $att_info."SPECRES9LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES9STOPDATE")
        {$att_info = $att_info."SPECRES9STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "SPECRES10LEFT")
        {$att_info = $att_info."SPECRES10LEFT&";$ret_info = $ret_info."0&";}
        elsif ($name eq "SPECRES10STOPDATE")
        {$att_info = $att_info."SPECRES10STOPDATE&";$ret_info = $ret_info."19900101&";}
        elsif ($name eq "CONSUMEACC6")
        {$att_info = $att_info."CONSUMEACC6&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CONSUMEACC7")
        {$att_info = $att_info."CONSUMEACC7&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CONSUMEACC8")
        {$att_info = $att_info."CONSUMEACC8&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CONSUMEACC9")
        {$att_info = $att_info."CONSUMEACC9&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CONSUMEACC10")
        {$att_info = $att_info."CONSUMEACC10&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CONSUMEACC11")
        {$att_info = $att_info."CONSUMEACC11&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CONSUMEACC12")
        {$att_info = $att_info."CONSUMEACC12&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CONSUMEACC13")
        {$att_info = $att_info."CONSUMEACC13&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CONSUMEACC14")
        {$att_info = $att_info."CONSUMEACC14&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CONSUMEACC15")
        {$att_info = $att_info."CONSUMEACC15&";$ret_info = $ret_info."0&";}
        elsif ($name eq "PKGRENTFLAG")
        {$att_info = $att_info."PKGRENTFLAG&";$ret_info = $ret_info."1010010000&";}
        elsif ($name eq "MULTISRVFLAGSTATE")
        {$att_info = $att_info."MULTISRVFLAGSTATE&";$ret_info = $ret_info."000010000000001001001000000000000100&";}
        elsif ($name eq "ASFLAGSTATE")
        {$att_info = $att_info."ASFLAGSTATE&";$ret_info = $ret_info."000000001000000000000000&";}
        elsif ($name eq "DATEFIELD1")
        {$att_info = $att_info."DATEFIELD1&";$ret_info = $ret_info."1990-01-01&";}
        elsif ($name eq "STRFIELD1")
        {$att_info = $att_info."STRFIELD1&";$ret_info = $ret_info."0000000000000000000000000000000000000000000000000000000000000000&";}
        elsif ($name eq "STRFIELD2")
        {$att_info = $att_info."STRFIELD2&";$ret_info = $ret_info."13911484687&";}
        elsif ($name eq "STRFIELD3")
        {$att_info = $att_info."STRFIELD3&";$ret_info = $ret_info."&";}
        elsif ($name eq "INTFIELD7")
        {$att_info = $att_info."INTFIELD7&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLINGINTERTIME")
        {$att_info = $att_info."CALLINGINTERTIME&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLINGOUTTIME")
        {$att_info = $att_info."CALLINGOUTTIME&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLEDINTERTIME")
        {$att_info = $att_info."CALLEDINTERTIME&";$ret_info = $ret_info."0&";}
        elsif ($name eq "CALLEDOUTTIME")
        {$att_info = $att_info."CALLEDOUTTIME&";$ret_info = $ret_info."0&";}
        elsif ($name eq "UNAME")
        {$att_info = $att_info."UNAME&";$ret_info = $ret_info."&";}
        elsif ($name eq "USERSEX")
        {$att_info = $att_info."USERSEX&";$ret_info = $ret_info."&";}
        elsif ($name eq "IDENTITYCARD")
        {$att_info = $att_info."IDENTITYCARD&";$ret_info = $ret_info."&";}
        elsif ($name eq "CARDADDRESS")
        {$att_info = $att_info."CARDADDRESS&";$ret_info = $ret_info."&";}
        elsif ($name eq "HOMEADDRESS")
        {$att_info = $att_info."HOMEADDRESS&";$ret_info = $ret_info."&";}
        elsif ($name eq "POSTNO")
        {$att_info = $att_info."POSTNO&";$ret_info = $ret_info."&";}
        elsif ($name eq "CALLINGNAME")
        {$att_info = $att_info."CALLINGNAME&";$ret_info = $ret_info."&";}
        elsif ($name eq "MINCONSUMELEVEL")
        {$att_info = $att_info."MINCONSUMELEVEL&";$ret_info = $ret_info."&";}
        elsif ($name eq "USERMSG")
        {$att_info = $att_info."USERMSG&";$ret_info = $ret_info."&";}
        elsif ($name eq "GRPMULTISRVFLAG")
        {$att_info = $att_info."GRPMULTISRVFLAG&";$ret_info = $ret_info."00000020000008000000100000000000000001200000101000000000000000000000000000000000000000000000000000000000000000000000000000000000&";}
        elsif ($name eq "OWEBORROWAMT")
        {$att_info = $att_info."OWEBORROWAMT&";$ret_info = $ret_info."0&";}
    }   
    
     if (substr($att_info, -1) eq "&"){
         substr($att_info, -1) = "";
         substr($ret_info, -1) = "";
     }
                                                    
     my $resp_desc = ": RETN=0, DESC=\"成功\",ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
     return $resp_desc;
}   
#LIST FVPN GRPLOG
sub ack_list_fvpn_grplog{
     my $att_info = "MAINNO&OPRTIME&GRPNAME&AREANO&OPERATORID&GRPOPRKIND&MEMO";
     my $ret_info = "12345&20110909090909&test&0001&111111&1&none";
     my $resp_desc = ": RETN=0, DESC=\"操作成功\",TOTAL=1,FINISHED=1,START=0,NUMOF=0,ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
     return $resp_desc;
}   
#LIST FVPN SUBUSER
sub ack_list_fvpn_subuser{
     my $att_info = "GROUPNUMBER&MEMBERNO&USERNAME&USERIDCARD&PCOMMENT&USERADDR&POWER&PHONENO";
     my $ret_info = "13910561217|13910561217|||||000000000000000000000000000000000000||&13910561217|13621234545|||||000000000000000000000000000000000000||&13910561217|13651256007|||||000000000000000000000000000000000000||&13910561217|13699119997|||||000000000000000000000000000000000000||";
     my $resp_desc = ": RETN=0, DESC=\"操作成功\",TOTAL=1,FINISHED=1,START=0,NUMOF=0,ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
     return $resp_desc;
}   
#DISP FVPN GRPINFO
sub ack_list_fvpn_grpinfo{
     my $att_info = "MAINNO&GRPTYPE&GRPSTATE&CREATETIME&SERVICESTART&SERVICESTOP&MSNUMBER&GRPAREA&FUNCFLAGS";
     my $ret_info = "111111&1&1&20110909090909&20110909&20110910&10&0001&000000000000000000000000000000000000";
     my $resp_desc = ": RETN=0, DESC=\"操作成功\",ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
     return $resp_desc;
}
#DISP IUSER FNTELNO
sub ack_list_iuser_fntelno{
     my $att_info = "FN1&FN2&FN3";
     my $ret_info = "13800000001&13800000002&13800000003";
     my $resp_desc = ": RETN=0, DESC=\"操作成功\",ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
     return $resp_desc;
}
#LIST IUSER CHRGINFO
sub ack_list_iuser_chrginfo{
     my $att_info = "TYPE&ACCOUNTPIN&TRADETIME&CHRGMSISDN&SEQUENCE&PREACCOUNTLEFT&PRECALLSERVICESTOP&PREACCOUNTSTOP&TRADETYPE&ERRORTYPE&APPINFO&ERRORNO&COUNTTOTAL&INTFIELD2&INTFIELD3&GIFTAMT&FREESMSGIFT&NROFGIFTMON&MONGIFTAMT&LASTMONGIFTAMT&GIFTSSTFLAG&MONGIFTSSTFLAG&RECHGCTRLFLAG&GIFTACCTLEFT&GIFTACCTSTOPDATE&REMITBEGINDATE&PRECASHAMT&PRECASHLEFT&NROFREMITMON&MONREMITAMT&OPERATORID&BATCH &SUPPLYCARDKIND&RECHGACC2INDEX&RECHGACC2STOPDATE&RECHGACC2LEFT&RECHGRES2&RECHGACC3INDEX&RECHGACC3STOPDATE&RECHGACC3LEFT&RECHGRES3&RECHGACC4INDEX&RECHGACC4STOPDATE&RECHGACC4LEFT&RECHGRES4&RECHGACC5INDEX&RECHGACC5STOPDATE&RECHGACC5LEFT&RECHGRES5&RECHGACC6INDEX&RECHGACC6STOPDATE&RECHGACC6LEFT&RECHGRES6&GIFTACC2INDEX&GIFTACC2STOPDATE&GIFTACC2LEFT&GIFTRES2&GROUPNO";
     my $ret_info = "0|888888888888888888|2011-07-26 22:11:46|13718998452|LL041342600648800000000181603617|0|2011-07-26|2011-10-26|q|0||0|5000|0|5000|0|0|0|0|0|0|0|0000000000000000|0|1990-01-01||0|0|0|0|||-1|-1::||0|0|-1::||0|0|-1::||0|0|-1::||0|0|-1::||0|0|-1::||0|0|40&0||2011-07-20 15:08:25|13718998452|11143040153432438|5|2013-03-12|2013-06-12|1|0||0|3000|5|3005|0|0|0|0|0|0|0|0000000000000000|0|1990-01-01||0|0|0|0|uacagt|20110711001|0|-1::||0|0|-1::||0|0|-1::||0|0|-1::||0|0|-1::||0|0|-1::||0|0|40&0||2011-07-12 21:21:28|13718998452|11605020242653751|697|2012-12-12|2013-03-14|1|0||0|3000|697|3697|0|0|0|0|0|0|0|0000000000000000|0|1990-01-01||0|0|0|0|uacagt|02168973264|0|-1::||0|0|-1::||0|0|-1::||0|0|-1::||0|0|-1::||0|0|-1::||0|0|40";
     my $resp_desc = ": RETN=0, DESC=\"操作成功\",TOTAL=1,FINISHED=1,START=0,NUMOF=0,ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
     return $resp_desc;
}
#QUERY CHRG CARDSTAT
sub ack_list_chrg_cardstat{
     my $att_info = "MSISDN&CTOTAL&ACTDAY&CRDFLG&TRADTYPE&TRADTIME&CRDDAY&SUPPLYCARDKIND";
     my $ret_info = "&5000&90&0&&&2013-06-30&0";
     my $resp_desc = ": RETN=0, DESC=\"操作成功\",ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
     return $resp_desc;
}
#DISP VPN MEMBER
sub ack_list_vpn_member{
     my $att_info = "GRPID&PHONENO&ISDNNO&USERNAME&IDCARD&DEPT&PCOMMENT&CLOSENO1&CLOSENO2&CLOSENO3&CLOSENO4&CLOSENO5&LOCKFLAG&FLAGS&STATUS&USERTYPE&MAXOUTNUM&CUROUTNUM&OUTGRP&FEEFLAG&LMTFEE&CURPKG&CPKGNAME&FEETYPE&NEXTPKG&NPKGNAME&ISDNTYPE&STARTTIME&USERPROPERTY&PROPERTYNAME&MAINMSISDN&MNCSERVICETYPE&MONTHNOW&TOTALFEE&OVERDUE&FEE1&FEE2&FEE3&FEE4&DURAT1&DURAT2&DURAT3&DURAT4&DURAT5&PTOTALFEE&POVERDUE&PFEE1&PFEE2&PFEE3&PFEE4&PFEE5&PFEE6&PKGDAY&PAYDAY&RENTTIME1&RENTTIME2&RENTTIME3&RENTTIME4&RENTFEE1&RENTFEE2";
     my $ret_info = "\"1000123323\"&\"561647\"&\"13910881647\"&\"\"&\"\"&\"QA\"&\"\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"00\"&\"440000442201000000000000000000000000\"&\"011000000000000000000000000000000000\"&\"0\"&\"50\"&\"0\"&\"0\"&\"0000\"&\"0\"&\"0\"&\"无任何资费套餐类型\"&\"0\"&\"0\"&\"无任何资费套餐类型\"&\"4\"&\"2009-04-27 13:32:19\"&\"0\"&\"无任何特性\"&\"\"&\"0\"&\"\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"2999-12-31\"&\"2009-03-01\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"&\"0\"";
     my $resp_desc = ": RETN=1003, DESC=\"操作成功\",ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
     return $resp_desc;
}
#LIST VPN1 PMEMBER
sub ack_list_vpn_pmember{
	my $att_info =
	"TOTAL=1,FINISHED=1,START=0,NUMOF=1,ATTR=ISDNNUMBER & FLAGS & USERPROPERTY & ROUTINGNUMBER & PBXROUTINGPREF & PBXNUMBER & RAILVIRTUALNO & MAINMSISDN & MNCSERVICETYPE";
	my $ret_info = 
	"13683101231|000001000000000000000000000000000000|1||010908||||0|";
	my $resp_desc = ":RETN=0,DESC=\"查询个人类业务用户信息成功\",ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
	return $resp_desc;
}
#DISP IUSER BORRECHG
sub ack_list_iuser_borrechg{
     my $att_info = "\"FLAG\"&\"MSISDN\"&\"BORROWRECHGAMT\"&\"BORROWRECHGTIME\"&\"OWEBORROWAMT\"&\"TRADETIME\"&\"INTFIELD1\"&\"DATEFIELD1\"&\"STRFIELD1\"";
     my $ret_info = "\"1\"&\"13681314292\"&\"1000\"&\"20110820173513\"&\"1000\"&\"\"&\"0\"&\"20111120\"&\"\"";
     my $resp_desc = ": RETN=0, DESC=\"操作成功\",ATTR=".$att_info.",RESULT=\"".$ret_info."\";";
     return $resp_desc;
}
#QUERY IUSER USERPWD
sub ack_list_iuser_userpwd{
     my $resp_desc = ": RETN=0, DESC=\"操作成功\",USERPWD=ABCDE;";
     return $resp_desc;
}

our $shutdown_flag=0;
sub all_exit_sig { 
    $shutdown_flag = 1;
}

$SIG{KILL} = \&all_exit_sig;
$SIG{INT} = \&all_exit_sig;
$SIG{QUIT} = \&all_exit_sig;
$SIG{TERM} = \&all_exit_sig;

our $server_port=16020;
$|= 1;   

sub handle_client{
    my ($client) = @_;
    LOOP:while(!$shutdown_flag){
         my $sh = new IO::Select($client) or last LOOP;
         $sh->can_read(1) or next LOOP;
         
         #接收8个字节命令头
         my $sc_head;
         $client->recv($sc_head, 8);
         if (length($sc_head) != 8){
             print_log("recv sc_head fail!");
             last LOOP;
         }
         my($sc_beg,$sc_len) =  unpack("a4a4", $sc_head);
         print_log("Len1:".$sc_len);
         $sc_len=hex($sc_len)+8;
         print_log("Len2:".$sc_len);
         
         #接收命令体  
         my $sc_body;
         $client->recv($sc_body, $sc_len);
         if (length($sc_body) != $sc_len){
             print_log("recv sc_body fail!");
             last LOOP;
         }
         
         my ($sc_ver,$sc_ter,$sc_sev,
          $sc_id,$sc_ctl,$sc_add,
          $sc_ses_id,$sc_ses_ctl,$sc_ses_add,$sc_mml);
         
         if ($sc_len == 12){ 
             ($sc_mml)=unpack("a*",$sc_body);
         }else{   
             ($sc_ver,$sc_ter,$sc_sev,
                 $sc_id,$sc_ctl,$sc_add,
                 $sc_ses_id,$sc_ses_ctl,$sc_ses_add,$sc_mml)=unpack("a4a8a8 a8a6a4 a8a6a4 a*",$sc_body);
         }
         
         
         my $sc_chk = substr($sc_mml, -8);
         $sc_mml = substr($sc_mml, 0, length($sc_mml)-8);
         print_log("MML:\n".$sc_mml);
         print_log("Recv:\n".$sc_head.$sc_body);
        
         my $sc_cmd = ""; 
         #判断命令
         if($sc_mml =~m/([\w\s]+):/){
            $sc_cmd = $1;
         }   
         if ($sc_mml eq "HBHB" ){
             $sc_cmd = "HBHB";
         }
         if (length($sc_cmd) <= 1){
             print_log("parse fail!");
             last LOOP;
         }
         
         print_log("Handle: ".$sc_cmd);
         #根据不同命令调用不同处理
         my $ack = "";
         if ($sc_cmd eq "HBHB"){
             my $ack_mml = "HBHB";
             my $ack_len = length($ack_mml);
             $ack = pack("a4a4 a*a8", $sc_beg,$ack_len,$ack_mml,$sc_chk);
         }else{
             my $resp_desc;
             if ($sc_cmd eq "DISP IUSER USERINFO"){ 
                 $resp_desc = ack_disp_iuser_userinfo($sc_mml);
             } elsif ($sc_cmd eq "LIST FVPN GRPLOG"){
                 $resp_desc = ack_list_fvpn_grplog($sc_mml);
             } elsif ($sc_cmd eq "LIST FVPN SUBUSER"){   
                 $resp_desc = ack_list_fvpn_subuser($sc_mml);
             } elsif ($sc_cmd eq "DISP FVPN GRPINFO"){
                 $resp_desc = ack_list_fvpn_grpinfo($sc_mml);
             } elsif ($sc_cmd eq "DISP IUSER FNTELNO"){
                 $resp_desc = ack_list_iuser_fntelno($sc_mml);
             } elsif ($sc_cmd eq "LIST IUSER CHRGINFO"){
                 $resp_desc = ack_list_iuser_chrginfo($sc_mml);  
             } elsif ($sc_cmd eq "QUERY CHRG CARDSTAT"){
                 $resp_desc = ack_list_chrg_cardstat($sc_mml);
             } elsif ($sc_cmd eq "DISP VPN MEMBER"){
                 $resp_desc = ack_list_vpn_member($sc_mml);
             } elsif ($sc_cmd eq "DISP IUSER BORRECHG"){
                 $resp_desc = ack_list_iuser_borrechg($sc_mml); 
             } elsif ($sc_cmd eq "QUERY IUSER USERPWD"){
                 $resp_desc = ack_list_iuser_userpwd($sc_mml);
			 } elsif ($sc_cmd eq "LIST VPN1 PMEMBER"){
				 #$resp_desc = ": RETN=1003, DESC=\"Succeeded\"";
				$resp_desc = ack_list_vpn_pmember($sc_mml);
			 } elsif ($sc_cmd eq "DELE FVPN SUBNO"){
				$resp_desc = ": RETN=0, DESC=\"Succeeded\"";
			 }
	      #elsif ($sc_cmd eq "CREATE IUSER GPRS"){ 
	      #   $resp_desc = ": RETN=1111,DESC=\" \"";	
	     #}
			 elsif($sc_cmd eq "ADD VPN1 PMEMBERS"){
				 $resp_desc = ": RETN=0, DESC=\"Succeeded\"";	
			 }
			 else{
				$resp_desc = ": RETN=0, DESC=\"Succeeded\"";
			 }
             my $ack_mml = "ACK:".$sc_cmd.$resp_desc;
             my $ack_len = 56+length($ack_mml);
             $ack_len = sprintf("%04x", $ack_len);   
             $ack = pack("a4a4 a4a8a8 a8a6a4 a8a6a4 a*a8", $sc_beg,$ack_len,
                 $sc_ver, $sc_ter, "     ACK",
                 $sc_id, $sc_ctl, $sc_add,   
                 $sc_ses_id, " TXEND", "    ",
                 $ack_mml,$sc_chk);
         }   
         print_log("Send:\n".$ack);  
         $client->send($ack);    
    }
    close $client;
}
    
#服 务
our $server = IO::Socket::INET->new(LocalPort => $server_port,
    Type => SOCK_STREAM,
    Reuse => 1,
    Listen => 10) or die "Couldn't be a tcp server on port $server_port: $!\n";


#登陆
LOOP: while (!$shutdown_flag) {
    my $client = $server->accept();
    defined($client) or next LOOP;
    print "accept a client!\n";
    my $t = Thread->new(\&handle_client, $client);
    $t->detach();  
}
sleep(3);
print_log("Server Shutdown");
close($server);
