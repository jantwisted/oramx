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
    my $sql = 'SELECT column_name, data_type, data_length, data_precision, data_scale, data_default, nullable  FROM USER_TAB_COLUMNS WHERE table_name = ?';
    my $pksql = 'SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = ? AND cons.constraint_type = ? AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner AND cols.owner = ?';
    my $pknamesql = 'SELECT cols.constraint_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = ? AND cons.constraint_type = ? AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner AND cols.owner = ?';
    my $sth = $dbh->prepare($sql);
    my @list;
    my @pklist;
    my @pkname;
    $sth->execute($db_object);

# get primary key
    my $sthpk = $dbh->prepare($pksql);
    $sthpk->execute($db_object, 'P', $db_schema);

# get constraint name
    my $sthpkname = $dbh->prepare($pknamesql);
    $sthpkname->execute($db_object, 'P', $db_schema);

    while (my @row = $sth->fetchrow_array) {
	push @list, @row;
    }
    while (my @row_pk = $sthpk->fetchrow_array){
	push @pklist, @row_pk;
    }
    while (my @row_pk_name = $sthpkname->fetchrow_array){
	push @pkname, @row_pk_name;
    }
    table_constructor(\@list, \@pklist, \@pkname);
    $dbh->disconnect();
    
}

sub primary_key_constraint{
    my ($pkcolumns, $name) = @_;
    my $_name = '';
    my $pk_query = '';
    foreach(@$name){ $_name = $_; }
    $pk_query = "\n\tCONSTRAINT $_name PRIMARY KEY (";
    for(@$pkcolumns){
	$pk_query .= $_." ASC";
	if(  \$_ == \$$pkcolumns[-1]  ) {
	    $pk_query .= ") NOT DROPPABLE";
	}else{
	    $pk_query .= ', ';
	}
    }
    return $pk_query;
}


sub primary_key_footer{
    my ($pkcolumns) = @_;
    my $pk_query = '';
    $pk_query = "\nSTORE BY(";
    for(@$pkcolumns){
	$pk_query .= $_." ASC";
	if(  \$_ == \$$pkcolumns[-1]  ) {
	    $pk_query .= ")";
	}else{
	    $pk_query .= ', ';
	}
    }
    return $pk_query;
    
}


sub table_constructor{

    my ($columns, $pk, $pkn) = @_;
    my $ddl_head = "CREATE TABLE $db_object\n(";
    my $ddl_tail = "\n)\n";
    my $i = 0;
    my $con = 0;
    my $master_ddl = '';
    my $meta = "LOCATION ".$location."\nATTRIBUTES BLOCKSIZE ".$blocksize.", EXTENT(".$extent1.", ".$extent2."), MAXEXTENTS ".$maxextents;
    for(@$columns){

	if (! defined $_){ next; }
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
	    if ($_ eq 'NUMERIC'){
		$con = 2;
	    }
	    $master_ddl = $master_ddl.$_;
	    $i++;
	    next;
	}elsif ($i==2){
	    if ($con != 1 and $con != 2){
		$master_ddl = $master_ddl."($_)";
	    }elsif($con == 2){
		$i++;
		next;
	    }
	    $con = 0;
	    $i++;
	    next;
	}elsif ($i==3 and $con==2){
	    $master_ddl .= "($_,";
	    $i++;
	    next;
	}elsif ($i==4 and $con==2){
	    $master_ddl .= "$_)";
	    $con = 0;
	    $i++;
	    next;
	}else{
	    if(  \$_ == \$$columns[-1]  ) {
		if ($_ eq 'N'){
		    $master_ddl = $master_ddl." NOT NULL";
		}
		if (@$pk){
		    $master_ddl .= ','.primary_key_constraint(\@$pk, \@$pkn);
		    $master_ddl = $ddl_head.$master_ddl.$ddl_tail.$meta.primary_key_footer(\@$pk).";";
		}else{
		    $master_ddl = $ddl_head.$master_ddl.$ddl_tail.$meta.";";
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
    
    print $master_ddl;
}


main();

