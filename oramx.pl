#!/usr/bin/perl -w
#<Oramx converts oracle to sqlmx.>
#Copyright (C) <2014> <Janith Perera>

#------------------------------------------------------------------------------
# Name     : Oramx/oramx.pl
# Language : Perl
# Authors  : Janith Perera, janith@member.fsf.org
# Function : Main program
# Usage    : See documentation
#------------------------------------------------------------------------------
#
#          This file is part of Oramx.
#
#          Oramx is free software: you can redistribute it and/or modify
#          it under the terms of the GNU General Public License as published by
#          the Free Software Foundation, either version 3 of the License, or
#          (at your option) any later version.
#
#          Oramx is distributed in the hope that it will be useful,
#          but WITHOUT ANY WARRANTY; without even the implied warranty of
#          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#          GNU General Public License for more details.
#
#          You should have received a copy of the GNU General Public License
#          along with Oramx. If not, see <http://www.gnu.org/licenses/>.
#
#------------------------------------------------------------------------------

use Config::Simple;
use strict;
use warnings;
use DBI;
use Getopt::Long;
use utf8;

my $VERSION = '0.3.6';
Config::Simple->import_from('oramx.conf', \my %Config);
my $cfg = new Config::Simple('oramx.conf');

my $db_user = $cfg->param('USER');
my $db_pass = $cfg->param('PASSWORD');
my $db_name = $cfg->param('DATABASE');
my $db_type = $cfg->param('OBJECT_TYPE');
my $db_host = $cfg->param('HOST');
my $db_object = $cfg->param('OBJECT');
my $db_port = $cfg->param('PORT');
my $db_schema = $cfg->param('SCHEMA_NAME');
my $log_file = "./error.log";
my $query_file = $cfg->param('OUTPUT_FILE');
my $debug = $cfg->param('DEBUG');
my $dbh;
my $dsn;
my $meta_query;
my $location = $cfg->param('LOCATION');
my $blocksize = $cfg->param('BLOCKSIZE');
my $extent1 = $cfg->param('PARAM1');
my $extent2 = $cfg->param('PARAM2');
my $maxextents = $cfg->param('MAXEXTENTS');
my $_objcounter = 0;


our %TYPE = (
    # Oracle to MX data types mapping goes here
    'VARCHAR2' => 'VARCHAR',
    'NUMBER' => 'NUMERIC',
    'DATE' => 'TIMESTAMP',
    'TIMESTAMP' => 'TIMESTAMP2',
    'RAW' => 'BLOB'
);


our %KEYWORDS = (
    # check if column name is a key word of sqlmx
    'RESULT' => 1,
    'VALUE' => 2,
    'YEAR' => 3,
    'LANGUAGE' => 4,
    'KEY' => 5,
    'ALIAS' => 6,
    'POSITION' => 7,
    'TIMESTAMP' => 8
);

 


open (QUERY, ">>$query_file") or die "Could not open $query_file: $!";

sub get_dbconnection{
    print "Trying to connect to database: dbi:Oracle:host=$db_host;sid=$db_name;port=$db_port\n";
    $dbh = DBI->connect("dbi:Oracle:host=$db_host;sid=$db_name;port=$db_port","$db_user","$db_pass");

}

sub obj_loader{
    print "Retrieving table information...\n";
    my $obj_list = 'SELECT TABLE_NAME  FROM ALL_TABLES WHERE OWNER = ?';
    my @list;
    my $sth = $dbh->prepare($obj_list);
    $sth->execute($db_schema);
    while (my @row = $sth->fetchrow_array) {
	push @list, @row;
    }
    return @list;

}

sub _tables{

    my $sql = 'SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, DATA_DEFAULT, NULLABLE  FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?';
    my $pksql = 'SELECT COLS.COLUMN_NAME FROM ALL_CONSTRAINTS CONS, ALL_CONS_COLUMNS COLS WHERE COLS.TABLE_NAME = ? AND CONS.CONSTRAINT_TYPE = ? AND CONS.CONSTRAINT_NAME = COLS.CONSTRAINT_NAME AND CONS.OWNER = COLS.OWNER AND COLS.OWNER = ?';
    my $pknamesql = 'SELECT COLS.CONSTRAINT_NAME FROM ALL_CONSTRAINTS CONS, ALL_CONS_COLUMNS COLS WHERE COLS.TABLE_NAME = ? AND CONS.CONSTRAINT_TYPE = ? AND CONS.CONSTRAINT_NAME = COLS.CONSTRAINT_NAME AND CONS.OWNER = COLS.OWNER AND COLS.OWNER = ?';
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
    _table_constructor(\@list, \@pklist, \@pkname);


}

sub _ref_constraints{
    my $refsql = qq{SELECT CONS.TABLE_NAME, CONS.CONSTRAINT_NAME, COLS2.COLUMN_NAME, COLS.TABLE_NAME, COLS.COLUMN_NAME
	FROM ALL_CONSTRAINTS CONS LEFT JOIN ALL_CONS_COLUMNS COLS ON COLS.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME
	LEFT JOIN ALL_CONS_COLUMNS COLS2 ON COLS2.CONSTRAINT_NAME = CONS.CONSTRAINT_NAME
	WHERE CONS.CONSTRAINT_TYPE= ? AND CONS.OWNER= ? AND COLS.OWNER= ? AND COLS2.OWNER= ?};
    my @reflist;
    my $i=0;
    my $master_ddl='';
    my $rth = $dbh->prepare($refsql);

    $rth->execute('R', $db_schema, $db_schema, $db_schema);

    while(my @row = $rth->fetchrow_array){
	push @reflist, @row;
    }

    for (@reflist){
	if ($i==0){
	    print "Dumping refcon $_...\n";
	    $_objcounter += 1;
	    $master_ddl .= "ALTER TABLE ".$_." ADD CONSTRAINT ";
	    $i++;
	    next;
	}elsif ($i==1){
	    $master_ddl .= $_." FOREIGN KEY ";
	    $i++;
	    next;
	}elsif ($i==2){
	    $master_ddl .= "($_) REFERENCES ";
	    $i++;
	    next;
	}elsif ($i==3){
	    $master_ddl .= $_." ";
	    $i++;
	    next;
	}elsif ($i==4){
	    $master_ddl .= "($_) DROPPABLE;\n\n\n";
	    $i=0;
	}
    }

    print QUERY $master_ddl;

}

sub _primary_key{
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


sub _primary_key_footer{
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


sub _table_constructor{

    my ($columns, $pk, $pkn) = @_;
    my $ddl_head = "CREATE TABLE $db_object\n(";
    my $ddl_tail = "\n)\n";
    my $i = 0;
    my $con = 0; # column type identifier
    my $_type_con = 0; # type condition to avoid conflicts
    my $master_ddl = '';
    my $meta = "LOCATION ".$location."\nATTRIBUTES BLOCKSIZE ".$blocksize.", EXTENT(".$extent1.", ".$extent2."), MAXEXTENTS ".$maxextents;
    for(@$columns){
	if (! defined $_){ $i++;  next; }
	# all conditions should be looped before proceed
	if ($con == 2) { # looping NUMERIC
	   if ($i == 3){ 
	       $master_ddl .= "($_,";
	       $i++;
	       next;
	   }elsif ($i == 4){
	       $master_ddl .= "$_)";
	       $con = 0;
	       $i++;
	       next;
	   }
	}
	if ($con == 3) { # looping TIMESTAMP2
	   if ($i == 4){ # try to change the condition
	       $i++;
	       $con = 0;
	       next;
	   }
	}
	# BLOB need to be tested
	if ($con == 4) { # looping TIMESTAMP2
	   if ($i == 4){ 
	       $i++;
	       $con = 0;
	       next;
	   }
	}


	# looping ends
	if ($i==0){
	    $con = 0; # to avoid anomalies
	    if (exists($KEYWORDS{$_})){
		$master_ddl = $master_ddl."\n\t\"".$_."\" ";
	    }else{
		$master_ddl = $master_ddl."\n\t".$_." ";
	    }
	    $i++;
	    next;
	}elsif ($i==1){
	    
	    while( my( $key, $value ) = each %TYPE ){
		if (index($_, $key) != -1 and $_type_con==0){
		    $_ = $value;
		    $_type_con = 1;
		}
	    }
	    $_type_con = 0;
	    if ($_ eq 'TIMESTAMP'){
		$con = 1;
	    }
	    if ($_ eq 'NUMERIC'){
		$con = 2;
	    }
	    if ($_ eq 'TIMESTAMP2'){
		$con = 3;
		$_ = 'TIMESTAMP';
	    }
	    if ($_ eq 'BLOB'){
		$con = 4;
	    }

	    $master_ddl = $master_ddl.$_;
	    $i++;
	    next;
	}elsif ($i==2){
	    if ($con != 1 and $con != 2 and $con != 3){
		$master_ddl = $master_ddl."($_)";
	    }elsif($con == 2 or $con == 3){
		$i++;
		next;
	    }
	    $con = 0;
	    $i++;
	    next;
	}elsif ($i==5){
	    $master_ddl .= " DEFAULT ".$_;
	    $i++;
	    next;
	}else{
	    if(  \$_ == \$$columns[-1]  ) {
		if ($_ eq 'N'){
		    $master_ddl = $master_ddl." NOT NULL";
		}
		if (@$pk){
		    $master_ddl .= ','._primary_key(\@$pk, \@$pkn);
		    $master_ddl = $ddl_head.$master_ddl.$ddl_tail.$meta._primary_key_footer(\@$pk).";\n\n\n\n";
		}else{
		    $master_ddl = $ddl_head.$master_ddl.$ddl_tail.$meta.";\n\n\n\n";
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
   
   print QUERY $master_ddl;   
   print "Dumping table $db_object...\n";
   $_objcounter += 1;
}

sub _sequence{
    my $seqsql = qq{
    SELECT SEQUENCE_NAME, MIN_VALUE, INCREMENT_BY, MIN_VALUE, MAX_VALUE  FROM USER_SEQUENCES
    };
    my @seqlist;
    my $i=0;
    my $master_ddl='';
    my $rth = $dbh->prepare($seqsql);

    $rth->execute();

    while(my @row = $rth->fetchrow_array){
	push @seqlist, @row;
    }
    for (@seqlist){
	if ($i==0){
	    print "Dumping sequence $_...\n";
	    $_objcounter += 1;
	    $master_ddl .= qq{CREATE SEQUENCE "$_" };
	    $i++;
	    next;
	}elsif ($i==1){
	    $master_ddl .= qq{LARGEINT START WITH $_ };
            $i++;
            next;
	}elsif ($i==2){
	    $master_ddl .= qq{INCREMENT BY $_ };
	    $i++;
	    next;
	}elsif ($i==3){
	    $master_ddl .= qq{MINVALUE $_ };
	    $i++;
	    next;
	}elsif ($i==4){
	    $master_ddl .= qq{MAXVALUE $_;\n};
	    $i=0;
	}
    }

    print QUERY $master_ddl;
    
}

sub main{
    get_dbconnection();
    if ($db_type eq 'TABLE'){
	my @obj_list = obj_loader();
	foreach(@obj_list){
	    $db_object = $_;
	    _tables(); 
	}
    }elsif($db_type eq 'REFCON'){
	_ref_constraints();
    }elsif($db_type eq 'SEQUENCE'){
	_sequence();
    }
}

sub intro{

    print qq{

OraMX $VERSION, a converter for sqlmx.
See 'README.md' for more information.
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.


};
    print QUERY "-- Generated by OraMX, version $VERSION\n";
    print QUERY "-- Authors: Janith Perera, janith\@member.fsf.org\n";
    print QUERY "-- License: GPL v2 or Later\n\n\n";

}

sub outro{

print qq{

Finished execution.
Number of converted objects : $_objcounter
See $query_file
Bye!

}

}


# main program
intro();
main();
$dbh->disconnect();
close QUERY;
outro();
