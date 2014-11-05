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

my $VERSION = '0.3.4';
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
    'DATE' => 'TIMESTAMP'
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
    my $con = 0;
    my $master_ddl = '';
    my $meta = "LOCATION ".$location."\nATTRIBUTES BLOCKSIZE ".$blocksize.", EXTENT(".$extent1.", ".$extent2."), MAXEXTENTS ".$maxextents;
    for(@$columns){

	if (! defined $_){ $i++; next; }
	if ($i==0){
	    $master_ddl = $master_ddl."\n\t".$_." ";
	    $i++;
	    next;
	}elsif ($i==1){
	    if (exists $TYPE{$_}){
		$_ = $TYPE{$_};
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


# main program
intro();
main();
$dbh->disconnect();
close QUERY;
print qq{

Finished execution.
Number of converted objects : $_objcounter
See $query_file
Bye!

}
