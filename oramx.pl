#!/usr/bin/perl -w                                                                                                                                           
use Config::Simple;
use strict;
use warnings;
use DBI;
use Term::ANSIColor;
use Getopt::Long;
use utf8;

Config::Simple->import_from('oramx.conf', \my %Config);
my $cfg = new Config::Simple('oramx.conf');

my $db_user = $cfg->param('USER');
my $db_pass = $cfg->param('PASSWORD');
my $db_name = $cfg->param('DATABASE');
my $db_type = $cfg->param('DATABASE_TYPE');
my $db_host = $cfg->param('HOST');
my $db_object = $cfg->param('OBJECT');
my $db_port = $cfg->param('PORT');
my $db_schema = $cfg->param('SCHEMA_NAME');
my $log_file = "./error.log";
my $query_file = "./query.sql";
my $debug = $cfg->param('DEBUG');
my $dbh;
my $dsn;
my $meta_query;
my $location = $cfg->param('LOCATION');
my $blocksize = $cfg->param('BLOCKSIZE');
my $extent1 = $cfg->param('PARAM1');
my $extent2 = $cfg->param('PARAM2');
my $maxextents = $cfg->param('MAXEXTENTS');

my %data = ('VARCHAR2', 'VARCHAR', 'NUMBER', 'NUMERIC', 'DATE', 'TIMESTAMP');


sub get_dbconnection{
        
    $dbh = DBI->connect("dbi:Oracle:host=$db_host;sid=$db_name;port=$db_port","$db_user","$db_pass");
    
}

sub main{
    get_dbconnection();
    my $sql = 'SELECT column_name, data_type, data_length, nullable  FROM USER_TAB_COLUMNS WHERE table_name = ?';
    my $sth = $dbh->prepare($sql);
    my @list;
    $sth->execute($db_object);
    while (my @row = $sth->fetchrow_array) {
	push @list, @row;
    }
    table_constructor(@list);
    $dbh->disconnect();
    
}

sub get_primary{
    
}

sub table_constructor{
    my $ddl_head = "CREATE TABLE $db_object\n(";
    my $ddl_tail = "\n\t)\n";
    my $i = 0;
    my $con = 0;
    print $ddl_head;
    my $master_ddl = '';

    for(@_){
	if ($i==0){
	    $master_ddl = $master_ddl."\n\t".$_." ";
	    $i++;
	    next;
	}elsif ($i==1){
	    if (exists $data{$_}){
		$_ = $data{$_};
	    }
	    if ($_ eq 'TIMESTAMP'){
		$con = 1;
	    }
	    $master_ddl = $master_ddl.$_;
	    $i++;
	    next;
	}elsif ($i==2){
	    if ($con != 1){
		$master_ddl = $master_ddl."($_)";
	    }
	    $con = 0;
	    $i++;
	    next;
	}else{
	    if(  \$_ == \$_[-1]  ) {
		if ($_ eq 'N'){
		    $master_ddl = $master_ddl." NOT NULL";
		}

	    }else{
		if ($_ eq 'N'){
		    $master_ddl = $master_ddl." NOT NULL,";
		}else{
		    $master_ddl = $master_ddl.",";
		}
	    }
	    $i=0;
	    next;
	}
    }
    my $meta = "LOCATION ".$location."\nATTRIBUTES BLOCKSIZE ".$blocksize.", EXTENT(".$extent1.", ".$extent2."), MAXEXTENTS ".$maxextents.";";
    $master_ddl = $master_ddl.$ddl_tail.$meta;
    print $master_ddl;
}


main();

