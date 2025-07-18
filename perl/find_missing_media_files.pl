#!/usr/bin/perl -w

use DBI;
use strict;

# my $dry_run = 0;
my $rename_files = 1;

my $sth;
my $driver   = "SQLite"; 
# my $database = "test.db";
my $database = "../ZebMoore_Ancestry.rmtree";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1, sqlite_unicode => 1 }) 
   or die $DBI::errstr;


if (! -r "unifuzz.so") {
   printf "ERROR: file unifuzz.so is not readable\n";
   $dbh->disconnect();
   exit(1);
}


$dbh->sqlite_enable_load_extension(1);
print "Loading extension\n";
$sth = $dbh->prepare("select load_extension('./unifuzz.so')") or die "Cannot prepare: " . $dbh->errstr();
my $rtn = $sth->execute() or die $DBI::errstr;
print "Extension loaded\n";



print "Opened database $database successfully\n";


my $stmt = qq(SELECT MediaID, MediaPath, MediaFile, MediaType from MultimediaTable;);
$sth = $dbh->prepare( $stmt );
my $rv = $sth->execute() or die $DBI::errstr;


if($rv < 0) {
   print $DBI::errstr;
}

# my $row2_count = 0;
my $LinkID;

while(my @row = $sth->fetchrow_array()) {

   # print "MediaID   = ". $row[0] . "\n";
   # print "MediaPath = ". $row[1] ."\n";
   # print "MediaFile = ". $row[2] ."\n";
   # print "MediaType = ". $row[3] ."\n";
   my $MediaID = $row[0]; 
   my $MediaFile = $row[2]; 
   my $MediaType = $row[3];
   my $relpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $MediaFile);

   unless ( -r "$relpath" ) {
      print "MediaID   = ". $row[0] . "\n";
      print "MediaPath = ". $row[1] ."\n";
      print "MediaFile = ". $row[2] ."\n";
      print "MediaType = ". $row[3] ."\n";
      printf "Error: MediaFile \"%s\" for MediaID: %d not readable\n", $relpath, $MediaID;

      # if ($MediaID eq 350) {
      #    print_multimedia_record($MediaID);
      # }

      # does alternate file exist?
      my $try1 = $relpath;
      $try1 =~ s/\.jpg$/.html/; 
      if ( -r "$try1" ) {
         printf "Info: MediaFile \"%s\" for MediaID: %d exists\n", $try1, $MediaID;
         $MediaFile =~ s/\.jpg$/.html/;  
         my $stmt = "";


         print_multimedia_record($MediaID);

         get_media_links($MediaID); 


         if ($MediaType ne 2) {
            $stmt = qq(UPDATE MultimediaTable SET MediaType = 2 MediaFile='$MediaFile' WHERE MediaID = $MediaID ;);
         } else {
            $stmt = qq(UPDATE MultimediaTable SET MediaFile='$MediaFile' WHERE MediaID=$MediaID ;);
         }
         printf "%s\n", $stmt;
         $sth = $dbh->prepare( $stmt );
         $rtn = $sth->execute() or die $DBI::errstr;
         if($rtn < 0) {
            print $DBI::errstr;
         }


         # $stmt = qq(UPDATE MultimediaTable SET MediaFile='$MediaFile' WHERE MediaID=$MediaID ;);
         # printf "%s\n", $stmt;
         # $sth = $dbh->prepare( $stmt );
         # $rtn = $sth->execute() or die $DBI::errstr;
         # if($rtn < 0) {
         #    print $DBI::errstr;
         # }


      }
   }

}

$dbh->disconnect();

exit(0);

sub print_multimedia_record {
   my($MediaID) = @_;

   my $stmt = qq(SELECT * from MultimediaTable WHERE MediaID=$MediaID;);
   printf "\n%s\n", $stmt;
   my $sth = $dbh->prepare( $stmt );
   my $rtn = $sth->execute() or die $DBI::errstr;
   if($rtn < 0) {
      print $DBI::errstr;
   }
   while(my @row = $sth->fetchrow_array()) {
      for(my $i = 0; $i <= $#row; $i++) {
         my $column_name = "";
         if ($i eq 0) {$column_name = "MediaID";}
         if ($i eq 1) {$column_name = "MediaType";}
         if ($i eq 2) {$column_name = "MediaPath";}
         if ($i eq 3) {$column_name = "MediaFile";}
         if ($i eq 4) {$column_name = "URL";}
         if ($i eq 5) {$column_name = "Thumbnail";}
         if ($i eq 6) {$column_name = "Caption";}
         if ($i eq 7) {$column_name = "RefNumber";}
         if ($i eq 8) {$column_name = "Date";}
         if ($i eq 9) {$column_name = "SortDate";}
         if ($i eq 10) {$column_name = "Description";}
         if ($i eq 11) {$column_name = "UTCModDate";}
         if ($i eq 5) {next;}
         print $i . " : " . $column_name . " : " . $row[$i] . "\n";
      }
   }
}

sub get_media_links {
   my($MediaID) = @_;

   my $stmt = qq(SELECT LinkID, MediaID, OwnerType, OwnerID FROM MediaLinkTable WHERE MediaID = $MediaID;); 
   printf("%s\n", $stmt);
   my $sth = $dbh->prepare( $stmt );
   my $rv = $sth->execute() or die $DBI::errstr;

   # How many rows where returned?
   my $row_count = 0;
   while(my @row = $sth->fetchrow_array()) {
      $row_count++;
   }
   printf "INFO: This many rows where returned: %0d\n", $row_count; 

   $sth = $dbh->prepare( $stmt );
   $rv = $sth->execute() or die $DBI::errstr;

   while(my @row = $sth->fetchrow_array()) {
      my $strOwnerType = "";
      if ($row[2] eq 0)  { $strOwnerType = "Person"; }
      if ($row[2] eq 1)  { $strOwnerType = "Family"; }
      if ($row[2] eq 2)  { $strOwnerType = "Event"; }
      if ($row[2] eq 3)  { $strOwnerType = "Source"; }
      if ($row[2] eq 4)  { $strOwnerType = "Citation"; }
      if ($row[2] eq 5)  { $strOwnerType = "Place"; }
      if ($row[2] eq 6)  { $strOwnerType = "Task"; }
      if ($row[2] eq 7)  { $strOwnerType = "Name (Primary or Alternate)"; }
      if ($row[2] eq 14) { $strOwnerType = "Place Details"; }
      if ($row[2] eq 19) { $strOwnerType = "Association"; }

      printf "   -------------------------------\n";
      $LinkID = $row[0];
      print  "   LinkID    = ". $row[0] . "\n";
      print  "   MediaID   = ". $row[1] . "\n";
      print  "   OwnerType = ". $row[2] . " : $strOwnerType\n";
      print  "   OwnerID   = ". $row[3] . "\n";

      # If a person, get the name associated with this person
      if ($row[2] eq 0) {
         my $stmt = qq(SELECT NameID, OwnerID, Surname, Given FROM NameTable WHERE OwnerID = $row[3];); 
         my $sth = $dbh->prepare( $stmt );
         my $rv = $sth->execute() or die $DBI::errstr;
         while(my @row3 = $sth->fetchrow_array()) {
            print "      NameID    = ". $row3[0] . "\n";
            print "      OwnerID   = ". $row3[1] . "\n";
            print "      Surname   = ". $row3[2] . "\n";
            print "      Given     = ". $row3[3] . "\n";
         }
      }

      # If a Citation
      if ($row[2] eq 4) {
         my $stmt = qq(SELECT * FROM CitationTable WHERE CitationID = $row[3];); 
         my $sth = $dbh->prepare( $stmt );
         my $rv = $sth->execute() or die $DBI::errstr;
         while(my @row4 = $sth->fetchrow_array()) {
            print "      CitationID    = ". $row4[0] . "\n";
            print "      SourceID      = ". $row4[1] . "\n";
            print "      Comments      = ". $row4[2] . "\n";
            print "      ActualText    = ". $row4[3] . "\n";
            print "      RefNumber     = ". $row4[4] . "\n";
            print "      Footnote      = ". $row4[5] . "\n";
            print "      ShortFootnote = ". $row4[6] . "\n";
            print "      Bibliography  = ". $row4[7] . "\n";
            print "      Fields        = ". $row4[8] . "\n";
            print "      UTCModDate    = ". $row4[9] . "\n";
            print "      CitationName  = ". $row4[10] . "\n";


            # Now print the SourceID record
            if ( $row4[1] gt 0 ) {
               my $stmt = qq(SELECT SourceID, Name FROM SourceTable WHERE SourceID = $row4[1];); 
               my $sth = $dbh->prepare( $stmt );
               my $rv = $sth->execute() or die $DBI::errstr;
               while(my @row5 = $sth->fetchrow_array()) {
                  print "         SourceID      = ". $row5[0] . "\n";
                  print "         Name          = ". $row5[1] . "\n";
               } 
            } 


         }
      }
   }
}
