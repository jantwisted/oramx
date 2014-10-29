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

#This file is part of Oramx.

#Oramx is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.

#Oramx is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#GNU General Public License for more details.

#You should have received a copy of the GNU General Public License
#along with Oramx. If not, see <http://www.gnu.org/licenses/>.


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
open (QUERY, ">>$query_file") or die "Could not open $query_file: $!";

sub get_dbconnection{
    print "Trying to connect to database: dbi:Oracle:host=$db_host;sid=$db_name;port=$db_port\n";    
    $dbh = DBI->connect("dbi:Oracle:host=$db_host;sid=$db_name;port=$db_port","$db_user","$db_pass");
    
}

sub obj_loader{
    print "Retrieving table information...";
    get_dbconnection();
    my $obj_list = 'SELECT table_name  from all_tables where owner = ?';
    my @list;
    my $sth = $dbh->prepare($obj_list);
    $sth->execute($db_schema);
    while (my @row = $sth->fetchrow_array) {
	push @list, @row;
    }
    $dbh->disconnect();
    return @list;
    
}

sub dbcon{
    
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

	if (! defined $_){ $i++; next; }
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
		    $master_ddl .= ','.primary_key_constraint(\@$pk, \@$pkn);
		    $master_ddl = $ddl_head.$master_ddl.$ddl_tail.$meta.primary_key_footer(\@$pk).";\n\n";
		}else{
		    $master_ddl = $ddl_head.$master_ddl.$ddl_tail.$meta.";\n\n";
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
    print "Dumping table $db_object...\n"
}


sub main{
    my @obj_list = obj_loader();
    get_dbconnection();
    foreach(@obj_list){
	$db_object = $_;
	dbcon();
    }
}

sub intro{
    print "\n";
    print "OraMX 0.3.0, a converter for sqlmx.\n";
    print "See 'README.md' for more information.\n";
    print "This is free software: you are free to change and redistribute it.\n";
    print "There is NO WARRANTY, to the extent permitted by law.\n";
    print "\n\n"
}


intro();
main();
$dbh->disconnect();
close QUERY;
