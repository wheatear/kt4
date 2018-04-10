#! /bin/perl 

use IO::Socket::INET;
use Time::HiRes qw(gettimeofday);

use POSIX qw(strftime);
use POSIX ":signal_h"; 
use Thread;
use IO::Select;
use strict;
use diagnostics;
#use File::Basename qw(fileparse)


#our ( $MYNAME,$MYDIR,$MYSUF ) = fileparse($0);


#our $CFGFILE = "${MYDIR}/${MYNAME}.cfg";
#our $LOG_NAME="${MYDIR}/${MYNAME}.log";

our $CFGFILE = $ARGV[0];
our $LOG_NAME="${CFGFILE}.log";

open(STDERR,">>$LOG_NAME") || die "can't open err log file $LOG_NAME: $!\n"; 
open(LOG,">>$LOG_NAME") || die "can't open log file $LOG_NAME: $!\n"; 
select LOG;
$| = 1;
select STDOUT;

#open(LOG,">>$LOG_NAME") or die(" Open $LOG_NAME error $!");

#our $server_port=8002;
#$server_port=8080;
$|=1;   

our $server_port;
our %resp_head;
our %resp_body;
our %map_req_resp;

#our $cfg_fh;
open my $cfg_fh, "< $CFGFILE" or die(" Open $CFGFILE error: $!");

our $section = "";
my @port;
my $key;
my $value;
my @map_req_resp;
while(<$cfg_fh>){
	chomp;
	next if (length()==0);
	print "$_\n";
	if ($_ eq "#service_port") {$section = "service_port" ;next;}
	if ($_ eq "#request command") {$section = "request command" ;next;}
	if ($_ eq "#response head") {$section = "response head" ;next;}
	if ($_ eq "#response body") {$section = "response body" ;next;}
	if ($_ eq "#mapping of request and response") {$section = "mapping of request and response" ;next;}
	
	if ($section eq "service_port") {
		@port = split / /,$_; 
		$server_port = $port[1];
		
#		print_log("port: $server_port ");
	}
	if ($section eq "response head") {
		$key = $_;
		chomp($value = <$cfg_fh>);

		$resp_head{$key} = $value;
		
#		print_log("resp_head: $key $value");
	}
	if ($section eq "response body") {
		$key = $_;
		chomp($value = <$cfg_fh>);

		$resp_body{$key} = $value;
		
#		print_log("resp_body: $key $value");
	}
	if ($section eq "mapping of request and response") {
		@map_req_resp = split / /,$_; 
		$map_req_resp{$map_req_resp[0]} = "$map_req_resp[1] $map_req_resp[2]";
		
#		print_log("map_req_resp: $map_req_resp{$map_req_resp[0]}");
	}
	
}

our $shutdown_flag=0;
sub all_exit_sig { 
    $shutdown_flag = 1;
}

$SIG{KILL} = \&all_exit_sig;
$SIG{INT} = \&all_exit_sig;
$SIG{QUIT} = \&all_exit_sig;
$SIG{TERM} = \&all_exit_sig;

#响应消息
our $response_head='HTTP/1.1 200 OK\r\nServer: Huawei web server\r\nContent-Type: text/xml; charset="utf-8"\r\nContent-Length: ';
our $response_head_login='HTTP/1.1 307 Temporary Redirect\r\nLocation: http://10.7.5.164:8002/01M1U6kBFFxv39uhbpNMyvRPcmC5yblhjKeJCCr88SfWZBIJBeNXGBVtvcUoUCP2\r\nServer: Huawei web server\r\nContent-Type: text/xml; charset="utf-8"\r\nContent-Length: ';

our $LGIResponse_body = '<?xml version="1.0" ?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><SOAP-ENV:Body><LGIResponse xmlns="http://www.huawei.com/HLR9820/LGI" ><Result><ResultCode>0</ResultCode><ResultDesc>SUCCESS0001:Operation is successful</ResultDesc></Result></LGIResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>';

#our $http_body_suc='<?xml version="1.0" ?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><SOAP-ENV:Body><HWHSSResponse xmlns="http://www.chinamobile.com/HSS/" ><Result><ResultCode>0</ResultCode><ResultDesc>SUCCESS0001:Operation is successful</ResultDesc></Result></HWHSSResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>';

our $resp_body_succomm='<?xml version="1.0" ?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><SOAP-ENV:Body><HWHSSCMDResponse xmlns="http://www.chinamobile.com/HSS/" ><Result><ResultCode>0</ResultCode><ResultDesc>SUCCESS0001:Operation is successful</ResultDesc></Result></HWHSSCMDResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>';
our $resp_body_err3037='<?xml version="1.0" ?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><SOAP-ENV:Body><HWHSSCMDResponse xmlns="http://www.chinamobile.com/HSS/" ><Result><ResultCode>3037</ResultCode><ResultDesc>ERR3037:Database updated but network update failure. Indication: operator should send cancellocation</ResultDesc></Result></HWHSSCMDResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>';

our $resp_LST_GPRS = '<?xml version="1.0" ?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><SOAP-ENV:Body><LST_GPRSResponse xmlns="http://www.chinamobile.com/HSS/" ><Result><ResultCode>0</ResultCode><ResultDesc>SUCCESS0001:Operation is successful</ResultDesc><ResultData><IMSI>460000694167613</IMSI><ISDN>8613910173348</ISDN><CHARGE_GLOBAL>PREPAID</CHARGE_GLOBAL><Group><CNTXID>1</CNTXID><QOSTPLID>33</QOSTPLID><PDPTYPE>IPV4</PDPTYPE><IPV4ADDIND>DYNAMIC</IPV4ADDIND><RELCLS>ACKALLPRODT</RELCLS><DELAYCLS>DELAY4</DELAYCLS><PRECLS>HIGH</PRECLS><PEAKTHR>256000 OCT</PEAKTHR><MEANTHR>BEST_EFFORT</MEANTHR><ARPRIORITY>NORMAL</ARPRIORITY><ERRSDU>NO</ERRSDU><DELIVERY>NO</DELIVERY><TRAFFICCLS>INTERACT</TRAFFICCLS><MAXSDUSIZE>1500 OCT</MAXSDUSIZE><MAXBRUPL>2048K</MAXBRUPL><MAXBRDWL>2048K</MAXBRDWL><RESBER>0.00001</RESBER><SDUERR>0.0001</SDUERR><TRANSFERDEL>10MS</TRANSFERDEL><TRAFFICPRI>PRIORITY2</TRAFFICPRI><MAXGBRUPL>384K</MAXGBRUPL><MAXGBRDWL>384K</MAXGBRDWL><APN>CMNET</APN><VPLMN>FALSE</VPLMN><CHARGE>NONE</CHARGE></Group><Group><CNTXID>2</CNTXID><QOSTPLID>33</QOSTPLID><PDPTYPE>IPV4</PDPTYPE><IPV4ADDIND>DYNAMIC</IPV4ADDIND><RELCLS>ACKALLPRODT</RELCLS><DELAYCLS>DELAY4</DELAYCLS><PRECLS>HIGH</PRECLS><PEAKTHR>256000 OCT</PEAKTHR><MEANTHR>BEST_EFFORT</MEANTHR><ARPRIORITY>NORMAL</ARPRIORITY><ERRSDU>NO</ERRSDU><DELIVERY>NO</DELIVERY><TRAFFICCLS>INTERACT</TRAFFICCLS><MAXSDUSIZE>1500 OCT</MAXSDUSIZE><MAXBRUPL>2048K</MAXBRUPL><MAXBRDWL>2048K</MAXBRDWL><RESBER>0.00001</RESBER><SDUERR>0.0001</SDUERR><TRANSFERDEL>10MS</TRANSFERDEL><TRAFFICPRI>PRIORITY2</TRAFFICPRI><MAXGBRUPL>384K</MAXGBRUPL><MAXGBRDWL>384K</MAXGBRDWL><APN>CMWAP</APN><VPLMN>FALSE</VPLMN><CHARGE>NONE</CHARGE></Group><Group><CNTXID>3</CNTXID><QOSTPLID>33</QOSTPLID><PDPTYPE>IPV4</PDPTYPE><IPV4ADDIND>DYNAMIC</IPV4ADDIND><RELCLS>ACKALLPRODT</RELCLS><DELAYCLS>DELAY4</DELAYCLS><PRECLS>HIGH</PRECLS><PEAKTHR>256000 OCT</PEAKTHR><MEANTHR>BEST_EFFORT</MEANTHR><ARPRIORITY>NORMAL</ARPRIORITY><ERRSDU>NO</ERRSDU><DELIVERY>NO</DELIVERY><TRAFFICCLS>INTERACT</TRAFFICCLS><MAXSDUSIZE>1500 OCT</MAXSDUSIZE><MAXBRUPL>2048K</MAXBRUPL><MAXBRDWL>2048K</MAXBRDWL><RESBER>0.00001</RESBER><SDUERR>0.0001</SDUERR><TRANSFERDEL>10MS</TRANSFERDEL><TRAFFICPRI>PRIORITY2</TRAFFICPRI><MAXGBRUPL>384K</MAXGBRUPL><MAXGBRDWL>384K</MAXGBRDWL><APN>CMMM</APN><VPLMN>FALSE</VPLMN><CHARGE>NONE</CHARGE></Group></ResultData></Result></LST_GPRSResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>';
our $resp_LST_SUB = '<?xml version="1.0" ?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><SOAP-ENV:Body><LST_SUBResponse xmlns="http://www.chinamobile.com/HSS/" ><Result><ResultCode>0</ResultCode><ResultDesc>SUCCESS0001:Operation is successful</ResultDesc><ResultData><Group><Group><HLRSN>5</HLRSN></Group><Group><IMSI>460001279036491</IMSI></Group><Group><ISDN>8613901193051</ISDN></Group><Group><NAM>BOTH</NAM><CATEGORY>COMMON</CATEGORY></Group></Group><Group Name ="LOCK" ><IC>FALSE</IC><OC>FALSE</OC><GPRSLOCK>FALSE</GPRSLOCK><EPSLOCK>FALSE</EPSLOCK><NON3GPPLOCK>FALSE</NON3GPPLOCK></Group><Group Name ="SABLOCK" ><IC>FALSE</IC><OC>FALSE</OC></Group><Group Name ="Basic Service" ><Group Name ="TSCODE:" ><TS>Telephony (TS11)</TS><TS>Emergency Call (TS12)</TS><TS>Short Message MT_PP (TS21)</TS><TS>Short Message MO_PP (TS22)</TS></Group></Group><Group Name ="DefaultCall" ><DEFAULTCALL>Telephony (TS11)</DEFAULTCALL></Group><Group Name ="ODB Data" ><ODBSS>FALSE</ODBSS><ODBOC>NOBOC</ODBOC><ODBIC>NOBIC</ODBIC><ODBPB1>FALSE</ODBPB1><ODBPB2>FALSE</ODBPB2><ODBPB3>TRUE</ODBPB3><ODBPB4>FALSE</ODBPB4><ODBENTE>FALSE</ODBENTE><ODBINFO>FALSE</ODBINFO><ODBROAM>NOBAR</ODBROAM><ODBRCF>BRICFEXHC</ODBRCF><ODBPOS>NOBPOS</ODBPOS></Group><Group Name ="RR Data" ><RROption>ALL_PLMNS</RROption></Group><Group Name ="SS Data" ><Group Name ="CFU" ><CFU>PROV</CFU><NCS>FALSE</NCS><COU>SUBSCRIBER</COU><Group><BSG>ALL</BSG><STATUS>PROV</STATUS></Group></Group><Group Name ="CFB" ><CFB>PROV</CFB><NFS>FALSE</NFS><NCS>FALSE</NCS><COU>SUBSCRIBER</COU><Group><BSG>ALL</BSG><STATUS>PROV</STATUS></Group></Group><Group Name ="CFNRY" ><CFNRY>PROV</CFNRY><NFS>FALSE</NFS><NCS>FALSE</NCS><COU>SUBSCRIBER</COU><Group><BSG>ALL</BSG><STATUS>PROV</STATUS></Group></Group><Group Name ="CFNRC" ><CFNRC>PROV</CFNRC><NCS>FALSE</NCS><COU>SUBSCRIBER</COU><Group><BSG>ALL</BSG><STATUS>PROV</STATUS></Group></Group><Group Name ="CFD" ><CFD>PROV</CFD><NFS>FALSE</NFS><NCS>FALSE</NCS><SUPINTERCFD>FALSE</SUPINTERCFD><VALIDCCF>CFNRc</VALIDCCF><Group><FTN>8613800100103</FTN><BSG>TS1X</BSG><STATUS>PROV | REG | ACT</STATUS><NotReplyTime>20</NotReplyTime></Group></Group><Group Name ="BAOC" ><BAOC>PROV</BAOC></Group><Group Name ="BOIC" ><BOIC>PROV</BOIC></Group><Group Name ="BOICEXHC" ><BOICEXHC>PROV</BOICEXHC></Group><Group Name ="BORO" ><BORO>NOTPROV</BORO></Group><Group Name ="BAIC" ><BAIC>PROV</BAIC></Group><Group Name ="BICROAM" ><BICROAM>PROV</BICROAM></Group><Group Name ="CW" ><CW>PROV</CW><Group><BSG>TS1X</BSG><STATUS>PROV | ACT</STATUS></Group></Group><Group Name ="PLMNSS" ><plmn-specificSS-1>NOTPROV</plmn-specificSS-1><plmn-specificSS-2>NOTPROV</plmn-specificSS-2><plmn-specificSS-3>NOTPROV</plmn-specificSS-3><plmn-specificSS-4>NOTPROV</plmn-specificSS-4><plmn-specificSS-5>NOTPROV</plmn-specificSS-5><plmn-specificSS-6>NOTPROV</plmn-specificSS-6><plmn-specificSS-7>NOTPROV</plmn-specificSS-7><plmn-specificSS-8>NOTPROV</plmn-specificSS-8><plmn-specificSS-9>NOTPROV</plmn-specificSS-9><plmn-specificSS-A>NOTPROV</plmn-specificSS-A><plmn-specificSS-B>NOTPROV</plmn-specificSS-B><plmn-specificSS-C>NOTPROV</plmn-specificSS-C><plmn-specificSS-D>NOTPROV</plmn-specificSS-D><plmn-specificSS-E>PROV</plmn-specificSS-E><plmn-specificSS-F>NOTPROV</plmn-specificSS-F><allplmn-specificSS>NOTPROV</allplmn-specificSS></Group><Group Name ="OTHER SS" ><CBCOU>SUBSCRIBER</CBCOU><CLIP>PROV</CLIP><CLIPOVERRIDE>FALSE</CLIPOVERRIDE><CLIR>NOTPROV</CLIR><COLP>NOTPROV</COLP><COLR>NOTPROV</COLR><HOLD>PROV</HOLD><MPTY>PROV</MPTY><EMLPP>NOTPROV</EMLPP><EMLPP_COU>SUBSCRIBER</EMLPP_COU></Group></Group><Group Name ="ARD" ><ARD>NOTPROV</ARD></Group><Group Name ="Other Service Data" ><CARP>NOTPROV</CARP></Group><Group Name ="PS Data" ><CHARGE_GLOBAL>PREPAID</CHARGE_GLOBAL><Group><CNTXID>1</CNTXID><APNQOSTPLID>511</APNQOSTPLID></Group><Group><CNTXID>2</CNTXID><APNQOSTPLID>510</APNQOSTPLID></Group><Group><CNTXID>3</CNTXID><APNQOSTPLID>513</APNQOSTPLID></Group></Group><Group Name ="EPS Data" ><CHARGE_GLOBAL>PREPAID</CHARGE_GLOBAL><AMBRMAXREQBWUL_GLOBAL>256000000</AMBRMAXREQBWUL_GLOBAL><AMBRMAXREQBWDL_GLOBAL>256000000</AMBRMAXREQBWDL_GLOBAL><DEFAULTCNTXID>1</DEFAULTCNTXID><Group><CNTXID>1</CNTXID><APNQOSTPLID>903</APNQOSTPLID></Group><Group><CNTXID>2</CNTXID><APNQOSTPLID>904</APNQOSTPLID></Group></Group><Group Name ="CSLocationData" ><VlrNum>8613441430</VlrNum><MscNum>8613441430</MscNum><MsPurgedForNonGprs>FALSE</MsPurgedForNonGprs></Group><Group Name ="PSLocationData" ><PURGEDONSGSN>FALSE</PURGEDONSGSN></Group><Group Name ="EPSLocationData" ><MMEHOST>mmec46.mmegi0103.mme.epc.mnc000.mcc460.3gppnetwork.org</MMEHOST><MMEREALM>epc.mnc000.mcc460.3gppnetwork.org</MMEREALM><PURGEDONMME>FALSE</PURGEDONMME></Group></ResultData></Result></LST_SUBResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>';
our $resp_LST_EPS = '<?xml version="1.0" ?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><SOAP-ENV:Body><LST_EPSResponse xmlns="http://www.chinamobile.com/HSS/" ><Result><ResultCode>0</ResultCode><ResultDesc>SUCCESS0001:Operation is successful</ResultDesc><ResultData><IMSI>460001279036491</IMSI><ISDN>8613901193051</ISDN><CHARGE_GLOBAL>PREPAID</CHARGE_GLOBAL><AMBRMAXREQBWUL_GLOBAL>256000000</AMBRMAXREQBWUL_GLOBAL><AMBRMAXREQBWDL_GLOBAL>256000000</AMBRMAXREQBWDL_GLOBAL><DEFAULTCNTXID>1</DEFAULTCNTXID><Group><CNTXID>1</CNTXID><PDNTYPE>IPV4IPV6</PDNTYPE><QOSTPLID>1</QOSTPLID><IPV4ADDIND>DYNAMIC</IPV4ADDIND><QOSCLASUID>9</QOSCLASUID><PRILEVEL>3</PRILEVEL><PREEMPTIONVUL>ENABLE</PREEMPTIONVUL><AMBRMAXUL>256000000</AMBRMAXUL><AMBRMAXDL>256000000</AMBRMAXDL><APN>CMNET</APN></Group><Group><CNTXID>2</CNTXID><PDNTYPE>IPV4IPV6</PDNTYPE><QOSTPLID>1</QOSTPLID><IPV4ADDIND>DYNAMIC</IPV4ADDIND><QOSCLASUID>9</QOSCLASUID><PRILEVEL>3</PRILEVEL><PREEMPTIONVUL>ENABLE</PREEMPTIONVUL><AMBRMAXUL>256000000</AMBRMAXUL><AMBRMAXDL>256000000</AMBRMAXDL><APN>CMWAP</APN></Group></ResultData></Result></LST_EPSResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>';
our $resp_LST_CFALL = '<?xml version="1.0" ?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><SOAP-ENV:Body><LST_CFALLResponse xmlns="http://www.chinamobile.com/HSS/" ><Result><ResultCode>0</ResultCode><ResultDesc>SUCCESS0001:Operation is successful</ResultDesc><ResultData><Group><IMSI>460001279036491</IMSI><ISDN>8613901193051</ISDN></Group><Group Name ="CFU" ><NCS>FALSE</NCS></Group><Group><FTN>&lt;NULL&gt;</FTN><BSG>ALL BS</BSG><STATUS>PROV</STATUS></Group><Group Name ="CFB" ><NFS>FALSE</NFS><NCS>FALSE</NCS></Group><Group><FTN>&lt;NULL&gt;</FTN><BSG>ALL BS</BSG><STATUS>PROV</STATUS></Group><Group Name ="CFNRY" ><NFS>FALSE</NFS><NCS>FALSE</NCS></Group><Group><FTN>&lt;NULL&gt;</FTN><BSG>ALL BS</BSG><STATUS>PROV</STATUS></Group><Group Name ="CFNRC" ><NCS>FALSE</NCS></Group><Group><FTN>&lt;NULL&gt;</FTN><BSG>ALL BS</BSG><STATUS>PROV</STATUS></Group><Group Name ="CFD" ><NFS>FALSE</NFS><NCS>FALSE</NCS><VALIDCCF>CFNRc</VALIDCCF></Group><Group><FTN>8613800100103</FTN><BSG>TS 1X</BSG><STATUS>PROV | REG | ACT</STATUS><NOREPTIME>20</NOREPTIME></Group></ResultData></Result></LST_CFALLResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>';
our $resp_LST_CBAR = '<?xml version="1.0" ?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><SOAP-ENV:Body><LST_CBARResponse xmlns="http://www.chinamobile.com/HSS/" ><Result><ResultCode>0</ResultCode><ResultDesc>SUCCESS0001:Operation is successful</ResultDesc><ResultData><IMSI>460001279036491</IMSI><ISDN>8613901193051</ISDN><Group Name ="BAOC" ><ALL_BS>PROV</ALL_BS></Group><Group Name ="BOIC" ><ALL_BS>PROV</ALL_BS></Group><Group Name ="BOICExHC" ><ALL_BS>PROV</ALL_BS></Group><Group Name ="BAIC" ><ALL_BS>PROV</ALL_BS></Group><Group Name ="BICROAM" ><ALL_BS>PROV</ALL_BS></Group></ResultData></Result></LST_CBARResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>';
#our $resp_LST_GPRS = '';
#our $resp_LST_GPRS = '';

my $xml_length_suc = length($resp_body_succomm);
my $xml_length_login = length($LGIResponse_body);
my $xml_length_err3037 = length($resp_body_err3037);

my $xml_length_LST_GPRS = length($resp_LST_GPRS);
my $xml_length_LST_SUB = length($resp_LST_SUB);
my $xml_length_LST_EPS = length($resp_LST_EPS);
my $xml_length_LST_CFALL = length($resp_LST_CFALL);
my $xml_length_LST_CBAR = length($resp_LST_CBAR);
#my $xml_length_LST_GPRS = length($resp_LST_GPRS);


our $response_login = $response_head_login.$xml_length_login."\r\n\r\n".$LGIResponse_body;
our $response_suc = $response_head.$xml_length_suc."\r\n\r\n".$resp_body_succomm;
our $response_err3037 = $response_head.$xml_length_err3037."\r\n\r\n".$resp_body_err3037;

our $response_LST_GPRS = $response_head.$xml_length_LST_GPRS."\r\n\r\n".$resp_LST_GPRS;
our $response_LST_SUB = $response_head.$xml_length_LST_SUB."\r\n\r\n".$resp_LST_SUB;
our $response_LST_EPS = $response_head.$xml_length_LST_EPS."\r\n\r\n".$resp_LST_EPS;
our $response_LST_CFALL = $response_head.$xml_length_LST_CFALL."\r\n\r\n".$resp_LST_CFALL;
our $response_LST_CBAR = $response_head.$xml_length_LST_CBAR."\r\n\r\n".$resp_LST_CBAR;

#服务
our $server = IO::Socket::INET->new(LocalPort => $server_port,
        Type => SOCK_STREAM,
        Reuse => 1,
        Listen => 20) or die "Couldn't be a tcp server on port $server_port: $!\n";

#登陆
my $i =0;
 while (!$shutdown_flag) {
    my ($client,$remot_addr) = $server->accept();
    defined($client) or next ;
    my $client_host = $client->peerhost();
    my $client_port = $client->peerport();
    
    &print_log("accept a client $client_host : $client_port");
    
    my $t =eval{ Thread->new(\&handle_client_comm, $client)};
    if ($@) {
    	&print_log("can't thread : $@");
    }
    $t->detach(); 
    
#    my @threads = Thread->list();
#    my $threadnum = scalar(@threads);
#    &print_log( "==========thread total: $threadnum");
#    foreach my $thd (@threads) {
#    	my $i_tid = $thd->tid();
#    	&print_log( "==========thread parent tid: $i_tid");
#    }
    $i++; 
#    $i > 20 && ($i = 0,sleep 2);
}
sleep(3);
print_log("Server Shutdown");
close($server);

fileno(LOG) == 0 || close LOG;  
exit;


#proc defin

sub print_log{
    my ($msg) = @_;
    
    if(fileno(LOG) == 0) {
    	open(LOG,">>$LOG_NAME") or die(" Open $LOG_NAME$!");
    	print "open log   =================================================\n";
  	}
    
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

}

sub handle_client_comm{
    my ($client) = @_;
    
    my $mythread = Thread->self();
    my $mytid = $mythread->tid();
    my $client_host = $client->peerhost();
    my $client_port = $client->peerport();
    &print_log( "$mytid ==========thread tid: $mytid $client_host : $client_port starting...");

    my $response;
    my $request;
    my $reqBuff;
    my $reqHead;
    my $headLength = 0;
    my $req_length = 0;

eval {
    while(!$shutdown_flag){
        $client->recv($reqBuff, 2000);
        my $buffLength = length($reqBuff);
        if ($buffLength<=0){
            &print_log("$mytid recv mml fail from $client_host : $client_port!");
            last;
        }
        &print_log("$mytid Recv: ".$reqBuff);
        
        my $resp_head;
    		my $resp_body;
    		my $head_key;
    		my $body_key;
        my $mached;
        
        $request = $request.$reqBuff;
        
        if ($headLength < 1) {
        	$headLength = index($request,"\r\n\r\n");
        	if ($headLength < 0) {
        		last:;
        	}
        	my $reqStart = $headLength + 4;
        	$reqHead = substr($request,0,$headLength);
        	$request = substr($request,$reqStart);
        	
        	$reqHead =~ /Content.Length: (\d+)/;
        
        	$req_length =$1;
        	&print_log("$mytid ------------req_length: $req_length ".$request);
        	if ($req_length == 0) {
#        	 	($head_key,$body_key) = split(/ /,$map_req_resp{"OTHER"});
        		$resp_head = $resp_head{"HB_HEAD"};
        	 	$resp_body = "";
        		$response = &gen_response($request,$resp_head,$resp_body);
        	 	&print_log("$mytid Send: ".$response);
           	$client->send($response);
           	$headLength = 0;
           	last;
        	}

        }

        my $recvLength = length($request);
        &print_log("$mytid recvlength : $recvLength!");
        if ($recvLength < $req_length) {
        	&print_log("$mytid recv : $recvLength  $req_length !");
        	next;
        }
        
        my $thisReq = substr($request,0,$req_length);
        $request = substr($request,$req_length);

        while( my ($req,$resp) = each( %map_req_resp) ){
        	&print_log("$mytid maping:  $req,$resp ");
        	if ($thisReq =~ /$req/){
        		&print_log("$mytid : request maped $req,$resp : $thisReq");
        		($head_key,$body_key) = split(/ /,$resp);
        		$resp_head = $resp_head{$head_key};
        		$resp_body = $resp_body{$body_key};
        		
            $response = &gen_response($thisReq,$resp_head,$resp_body);
            $mached = 1;
            &print_log("------------------$mytid Send: $head_key,$body_key ,$resp_head");
#            while (my ($a,$b) = each( %map_req_resp)){
#            }
            last;
        	}
        }
        if (!defined($mached)) {
        	&print_log("$mytid : request no maped, and send other response.");
        	($head_key,$body_key) = split(/ /,$map_req_resp{"OTHER"});
        		$resp_head = $resp_head{$head_key};
        		$resp_body = $resp_body{$body_key};
            $response = &gen_response($thisReq,$resp_head,$resp_body);
            
            &print_log("--------------------$mytid Send: $head_key,$body_key ,$resp_head");
        } else {
        	while( my ($req,$resp) = each( %map_req_resp) ){
        		&print_log("------------------$mytid pass: $req,$resp");
        	}
        }
   
        &print_log("$mytid Send: ".$response);
        $client->send($response);
        $headLength = 0;
    }
    
    close $client; 
  };
    if ($@) {
    	&print_log("can't receive socket : $@");
    }
    
    &print_log( "$mytid ==========thread tid: $mytid $client_host : $client_port return.");
    return;  
}   

sub handle_client{
    my ($client) = @_;
    
    my $mythread = Thread->self();
    my $mytid = $mythread->tid();
    my $client_host = $client->peerhost();
    my $client_port = $client->peerport();
    &print_log( "$mytid ==========thread tid: $mytid $client_host : $client_port starting...");
#    $client->detach(); 
#    Thread->self->detach;
    my $response;
    my $request;
    my $sh;
    while(!$shutdown_flag){
#        $sh = new IO::Select($client) or last;
#        $sh->can_read(1) or next;
        
        $client->recv($request, 4000);
        if (length($request)<=0){
            &print_log("$mytid recv mml fail from $client_host : $client_port!");
            last;
        }
        &print_log("$mytid Recv: ".$request);
        if ($request=~ /LGI/){
            $response = $response_login;
        }elsif ($request=~ /LST_GPRS/){
            $response = &gen_response($request,$response_head,$resp_LST_GPRS);
        }elsif ($request=~ /LST_SUB/){
            $response = &gen_response($request,$response_head,$resp_LST_SUB);
        }elsif ($request=~ /LST_EPS/){
            $response = &gen_response($request,$response_head,$resp_LST_EPS);
        }elsif ($request=~ /LST_CFALL/){
            $response = &gen_response($request,$response_head,$resp_LST_CFALL);
        }elsif ($request=~ /LST_CBAR/){
            $response = &gen_response($request,$response_head,$resp_LST_CBAR);
        }elsif ($request=~ /MOD_CB>/){
            $response = &gen_response($request,$response_head,$resp_body_succomm);
        }else{
            $response = &gen_response($request,$response_head,$resp_body_succomm);
        }    
        &print_log("$mytid Send: ".$response);
        $client->send($response);
    }
    
    close $client; 
    &print_log( "$mytid ==========thread tid: $mytid $client_host : $client_port return.");
    return;  
}   


sub print_hex{
    my($msg) = @_;
    my @bits = unpack("C*", $msg);
    my $i = 0;
    foreach my $bit (@bits){
        printf("%02x ", $bit);
        $i++;
        if ($i == 16){
            $i=0;
            print "\n";
        }
    }
    print "\n";
}

sub gen_response{
	my($request,$head,$body) = @_;
	
	my $patt_isdn_req = "<hss:ISDN>(\\d+)</hss:ISDN>";
	my $patt_imsi_req = "<hss:IMSI>(\\d+)</hss:IMSI>";
	my $patt_isdn_resp = "<ISDN>\\d+</ISDN>";
	my $patt_imsi_resp = "<IMSI>\\d+</IMSI>";
	my $patt_cmd_req = "<soapenv:Body>\\W*<hss:(\\w+)>";
	my $patt_cmd_resp = "HWHSSCMD";
	
	my $resp_isdn = "";
	my $resp_imsi = "";
	my $resp_cmd = "";
	
	$resp_isdn = "<ISDN>$1</ISDN>" if($request =~ /$patt_isdn_req/ );
	$resp_imsi = "<IMSI>$1</IMSI>" if($request =~ /$patt_imsi_req/ );
	$resp_cmd = "$1" if($request =~ /$patt_cmd_req/ );
	
	length($resp_isdn) > 0 && $body =~ s/$patt_isdn_resp/$resp_isdn/ge;
	length($resp_imsi) > 0 && $body =~ s/$patt_imsi_resp/$resp_imsi/ge;
	length($resp_cmd) > 0 && $body =~ s/$patt_cmd_resp/$resp_cmd/ge;
	
	my $body_length = length($body);
	my $reponse = $head.$body_length."\r\n\r\n".$body;
	return $reponse;
}

sub get_reqcmd{
	my($xml_msg) = @_;
	
	my $cmd_pattern = "<soapenv:Body>\\W*<hss:(\\w+)>";
#	&print_log("$xml_msg\n");
#	&print_log("$cmd_pattern\n");
	$xml_msg =~ /$cmd_pattern/ && return $1;
	return "";
}

#sub gen_response{
#	my($request,$head,$body) = @_;
#	
#	my $cmd = &get_reqcmd($request);
#	
#	$body =~ s/HWHSSCMD/$cmd/ge;
#	my $body_length = length($body);
#	my $reponse = $head.$body_length."\r\n\r\n".$body;
#	return $reponse;
#}


sub gen_lst_response{
	my($request,$head,$body) = @_;
	
	my $patt_isdn_req = "<hss:ISDN>(\\d+)</hss:ISDN>";
	my $patt_imsi_req = "<hss:IMSI>(\\d+)</hss:IMSI>";
	my $patt_isdn_resp = "<ISDN>\\d+</ISDN>";
	my $patt_imsi_resp = "<IMSI>\\d+</IMSI>";
	
	my $resp_isdn = "";
	my $resp_imsi = "";
	
	$resp_isdn = "<ISDN>$1</ISDN>" if($request =~ /$patt_isdn_req/ );
	$resp_imsi = "<IMSI>$1</IMSI>" if($request =~ /$patt_imsi_req/ );
	
	length($resp_isdn) > 0 && $body =~ s/$patt_isdn_resp/$resp_isdn/ge;
	length($resp_imsi) > 0 && $body =~ s/$patt_imsi_resp/$resp_imsi/ge;
	
	my $body_length = length($body);
	my $reponse = $head.$body_length."\r\n\r\n".$body;
	return $reponse;
}
