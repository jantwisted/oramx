#!/usr/bin/perl -w

# Oramx.pm - perl package for Oramx
# Copyright (C) 2014 Oramx, Janith Perera (janith@member.fsf.org)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

package Oramx;
use Config::Simple;
use strict;
use warnings;
use DBI;
use utf8;


my $VERSION = '1.0.0'; 



sub _init
{
    my $self = shift;
    Config::Simple->import_from($self->{config}, \my %Config) or die "Unable to open file $self->{config}: $!";  
    my $cfg = new Config::Simple($self->{config});
    $self->{db_user} = $cfg->param('USER');
    $self->{db_pass} = $cfg->param('PASSWORD');
    $self->{db_name} = $cfg->param('DATABASE');
    $self->{db_type} = $cfg->param('OBJECT_TYPE');
    $self->{db_host} = $cfg->param('HOST');
    $self->{db_object} = $cfg->param('OBJECT');
    $self->{db_port} = $cfg->param('PORT');
    $self->{db_schema} = $cfg->param('SCHEMA_NAME');
    $self->{log_file} = "./error.log";
    if (! defined $self->{query_file} ){ $self->{query_file} = $cfg->param('OUTPUT_FILE');  }     
    $self->{debug} = $cfg->param('DEBUG');
    $self->{meta_query} = ();
    $self->{location} = $cfg->param('LOCATION');
    $self->{blocksize} = $cfg->param('BLOCKSIZE');
    $self->{extent1} = $cfg->param('PARAM1');
    $self->{extent2} = $cfg->param('PARAM2');
    $self->{maxextents} = $cfg->param('MAXEXTENTS');
    $self->{_objcounter} = 0;
    open ($self->{QUERY}, ">>$self->{query_file}") or die "Could not open $self->{query_file}: $!";
    $self->{dbh} = (); 
    if ($cfg->param('INCLUDE')){
	$self->{include} = [ $cfg->param('INCLUDE') ];
	$self->{_INCLUDE}->{$_}++ for (@{ $self->{include}});
    }
    if ($cfg->param('EXCLUDE')){
	$self->{exclude} = [ $cfg->param('EXCLUDE') ];
	$self->{_EXCLUDE}->{$_}++ for (@{ $self->{exclude}});    
    }
    if ($cfg->param('ROW_BUFFER')){
	$self->{row_buffer} = $cfg->param('ROW_BUFFER');
    }else{
	$self->{row_buffer} = 1000;
    }
    if ($cfg->param('WHERE')){
	$self->{where_clause} = "where ".$cfg->param('WHERE');
    }else{
	$self->{where_clause} = "where 1=1";
    }
}

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
 
  
 
sub get_dbconnection{

    my $self = shift;
    print "Trying to connect to database: dbi:Oracle:host=$self->{db_host};sid=$self->{db_name};port=$self->{db_port}\n";
    $self->{dbh} = DBI->connect("dbi:Oracle:host=$self->{db_host};sid=$self->{db_name};port=$self->{db_port}","$self->{db_user}","$self->{db_pass}");
 
}
 
sub obj_loader{
    
    my $self = shift;
    print "Retrieving table information...\n";
    my $obj_list = 'SELECT TABLE_NAME  FROM ALL_TABLES WHERE OWNER = ?';
    my @list;
    my $sth = $self->{dbh}->prepare($obj_list);
    $sth->execute($self->{db_schema});
    while (my @row = $sth->fetchrow_array) {
       push @list, @row;
    }
    return @list;
 
}


sub _tables{
 
    my $self = shift;
    ($self->{db_object}) = @_;
    my $sql = 'SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, DATA_DEFAULT, NULLABLE  FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?';
    my $pksql = 'SELECT COLS.COLUMN_NAME FROM ALL_CONSTRAINTS CONS, ALL_CONS_COLUMNS COLS WHERE COLS.TABLE_NAME = ? AND CONS.CONSTRAINT_TYPE = ? AND CONS.CONSTRAINT_NAME = COLS.CONSTRAINT_NAME AND CONS.OWNER = COLS.OWNER AND COLS.OWNER = ?';
    my $pknamesql = 'SELECT COLS.CONSTRAINT_NAME FROM ALL_CONSTRAINTS CONS, ALL_CONS_COLUMNS COLS WHERE COLS.TABLE_NAME = ? AND CONS.CONSTRAINT_TYPE = ? AND CONS.CONSTRAINT_NAME = COLS.CONSTRAINT_NAME AND CONS.OWNER = COLS.OWNER AND COLS.OWNER = ?';
    my $sth = $self->{dbh}->prepare($sql);
    my @list;
    my @pklist;
    my @pkname;
    $sth->execute($self->{db_object}) or die $DBI::errstr;
 
 # get primary key
     my $sthpk = $self->{dbh}->prepare($pksql);
     $sthpk->execute($self->{db_object}, 'P', $self->{db_schema}) or die $DBI::errstr;
 
 # get constraint name
     my $sthpkname = $self->{dbh}->prepare($pknamesql);
     $sthpkname->execute($self->{db_object}, 'P', $self->{db_schema}) or die $DBI::errstr;
 
     while (my @row = $sth->fetchrow_array) {
 	push @list, @row;
     }
     while (my @row_pk = $sthpk->fetchrow_array){
 	push @pklist, @row_pk;
     }
     while (my @row_pk_name = $sthpkname->fetchrow_array){
 	push @pkname, @row_pk_name;
     }
     $self->_table_constructor(\@list, \@pklist, \@pkname);
 
}
 
sub _ref_constraints{
 
    my $self = shift;
    my $refsql = qq{SELECT CONS.TABLE_NAME, CONS.CONSTRAINT_NAME, COLS2.COLUMN_NAME, COLS.TABLE_NAME, COLS.COLUMN_NAME
 	FROM ALL_CONSTRAINTS CONS LEFT JOIN ALL_CONS_COLUMNS COLS ON COLS.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME
 	LEFT JOIN ALL_CONS_COLUMNS COLS2 ON COLS2.CONSTRAINT_NAME = CONS.CONSTRAINT_NAME
 	WHERE CONS.CONSTRAINT_TYPE= ? AND CONS.OWNER= ? AND COLS.OWNER= ? AND COLS2.OWNER= ?};
    my @reflist;
    my $i=0;
    my $master_ddl='';
    my $rth = $self->{dbh}->prepare($refsql);
 
    $rth->execute('R', $self->{db_schema}, $self->{db_schema}, $self->{db_schema}) or die $DBI::errstr;
 
    while(my @row = $rth->fetchrow_array){
       push @reflist, @row;
    }
 
    for (@reflist){
	if ($i==0){
	    print "Dumping refcon $_...\n";
	    $self->{_objcounter} += 1;
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
 
    print {$self->{QUERY}} $master_ddl;

}
 
sub _primary_key{
 
    my $self = shift;
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
    
    my $self = shift;
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
    
    my $self = shift;
    my ($columns, $pk, $pkn) = @_;
    my $ddl_head = "CREATE TABLE ".$self->{db_object}."\n(";
    my $ddl_tail = "\n)\n";
    my $i = 0;
    my $con = 0; # column type identifier
    my $_type_con = 0; # type condition to avoid conflicts
    my $master_ddl = '';
    my $meta = "LOCATION ".$self->{location}."\nATTRIBUTES BLOCKSIZE ".$self->{blocksize}.", EXTENT(".$self->{extent1}.", ".$self->{extent2}."), MAXEXTENTS ".$self->{maxextents};
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
 		    $master_ddl .= ','.$self->_primary_key(\@$pk, \@$pkn);
 		    $master_ddl = $ddl_head.$master_ddl.$ddl_tail.$meta.$self->_primary_key_footer(\@$pk).";\n\n\n\n";
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
    
    print {$self->{QUERY}} $master_ddl;   
    print "Dumping table $self->{db_object}...\n";
    $self->{_objcounter} += 1;
}
 
sub _sequence{
 
    my $self = shift;
    my $seqsql = qq{
     SELECT SEQUENCE_NAME, MIN_VALUE, INCREMENT_BY, MIN_VALUE, MAX_VALUE  FROM USER_SEQUENCES
     };
    my @seqlist;
    my $i=0;
    my $master_ddl='';
    my $rth = $self->{dbh}->prepare($seqsql);

    $rth->execute();
    
    while(my @row = $rth->fetchrow_array){
 	push @seqlist, @row;
    }
    for (@seqlist){
 	if ($i==0){
 	    print "Dumping sequence $_...\n";
 	    $self->{_objcounter} += 1;
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
    
    print {$self->{QUERY}} $master_ddl;
    
}
 
sub _init_insert{
     
    my $self = shift;
    my $initsql = qq{
     alter session set nls_date_format='YYYY-MM-DD'
     };
    my $init_db = $self->{dbh}->prepare($initsql);
    $init_db->execute();

}
 
sub _insert{
 
    my $self = shift;
    my ($_table) = @_;
    print {$self->{QUERY}} "\n------------ $_table ------------\n\n";
    my $rowsql = qq{
         SELECT * FROM $_table $self->{where_clause}
         };
    my $outer_str = "INSERT INTO $_table ";
    my $inner_str = undef;
    my $i=0;
    my $_total=0;
    my $master_ddl='';
    my $getRows = $self->{dbh}->prepare($rowsql);
    $getRows->execute();
    my $columns = $getRows->{NAME_lc};
    my ($rowref, $ref, @colarray);
    # outer wrapper
    $outer_str .= "(";
    $i=0; # initializing flag
    foreach(@$columns){ 
 	if ($i == $#$columns)
 	{
 	    push(@colarray, $_);
 	    $outer_str .= uc $_;
 	}else{
 	    push(@colarray, $_);
 	    $outer_str .= uc $_.",";
 	}
 	$i++;
    }
    $outer_str .= ") VALUES (";
    while($ref = $getRows->fetchall_arrayref(undef, $self->{row_buffer}))
    {
 	print "Dumping inserts $_table...\n";   
 	foreach $rowref (@$ref)
 	{
 	    $i=0; # initializing flag
  	    foreach (@$rowref){
 		if ($i == $#$rowref){
 		    $inner_str .= $self->_insert_get_columns($_, $self->_get_column_type(uc $colarray[$i], $_table)).")\n;\n";
 		}else{
 		    $inner_str .= $self->_insert_get_columns($_, $self->_get_column_type(uc $colarray[$i], $_table)).",";
 		}
 	        $i++;
 	    }
	    
 	    print {$self->{QUERY}} $outer_str.$inner_str;
 	    $_total += 1;
	    
 	    # initializing $inner_str
 	    $inner_str = undef;
 	}
	
    }
    print "$_table, total row count: $_total...\n";
    $self->{_objcounter} += 1;
     
}
 
sub _get_column_type{
    
    my $self = shift; 
    my ($_column, $_table) = @_;
    my @row;
    my $colsql = qq{ 
     SELECT DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?
     AND COLUMN_NAME = ?
     };
    my $ctype = $self->{dbh}->selectrow_array($colsql, undef, $_table, $_column);
    return $ctype;
    
}
 
sub _insert_get_columns{
 
    my $self = shift;
    my ($_column, $_type) = @_;
    if ($_type =~ /^TIMESTAMP/){ $_type = 'TIMESTAMP'; }
    if ($_type =~ /^CHAR/){ $_type = 'VARCHAR2'; }
    $_type = $TYPE {$_type};
    if ( $_type eq 'VARCHAR' ){
 	if (! defined $_column) { return "''"; }
 	else { return "'".$_column."'"; }
    }elsif ( $_type eq 'TIMESTAMP' ){
 	if (! defined $_column) { return "NULL"; }
 	else { return "TIMESTAMP '$_column:00:00:00'"; }
    }elsif ( $_type eq 'TIMESTAMP2' ){
 	if (! defined $_column) { return "NULL"; }
 	else { return "CURRENT"; }	
    }else{
 	if (! defined $_column) { return "''"; }
 	else { return $_column; }
    }
    
    
}
 
sub main{
  
    my $self = shift;
    $self->intro();
    $self->get_dbconnection();
    if ($self->{db_type} eq 'TABLE'){
 	my @obj_list = $self->obj_loader();
 	foreach(@obj_list){
 	    if ($self->{_INCLUDE}){	
		if (exists($self->{_INCLUDE}->{$_})){
 		    $self->_tables($_);
 		}
  	    }elsif($self->{_EXCLUDE}){
 		if (! exists($self->{_EXCLUDE}->{$_})){
 		    $self->_tables($_);
 		}
 	    }else{

 		$self->_tables($_);
 	    }
 	}
    }elsif($self->{db_type} eq 'REFCON'){
 	$self->_ref_constraints();
    }elsif($self->{db_type} eq 'SEQUENCE'){
 	$self->_sequence();
    }elsif($self->{db_type} eq 'INSERT'){
 	$self->_init_insert();
 	my @obj_list = $self->obj_loader();
 	foreach(@obj_list){
	    
 	    if ($self->{_INCLUDE}){
 		if (exists($self->{_INCLUDE}->{$_})){
 		    $self->_insert($_);
 		}
 	    }elsif($self->{_EXCLUDE}){
 		if (! exists($self->{_EXCLUDE}->{$_})){
 		    $self->_insert($_);
 		}
 	    }else{
 		$self->_insert($_);
 	    }
 	}
    }
}
 
sub intro{

    my $self = shift;
     print qq{
 
 OraMX $VERSION, a converter for sqlmx.
 See 'README.md' for more information.
 This is free software: you are free to change and redistribute it.
 There is NO WARRANTY, to the extent permitted by law.
 
 
};
     print {$self->{QUERY}} "-- Generated by OraMX, version $VERSION\n";
     print {$self->{QUERY}} "-- Authors: Janith Perera, janith\@member.fsf.org\n";
     print {$self->{QUERY}} "-- License: GPL v2 or Later\n\n\n";
 
}
 
sub outro{
 
    my $self = shift;
    print qq{
 
 Finished execution.
 Number of converted objects : $self->{_objcounter}
 See $self->{query_file}
 Bye!
 
}
 
}
 
sub DESTROY
{
    my $self = shift;
    $self->{dbh}->disconnect();
    $self->outro();
}
 

sub new{
    my $class = shift;
    my $self = {@_};
    bless($self, $class);
    $self->_init;
    return $self;
}


1;
