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

reindex_rmnocase();


my $stmt = qq(SELECT MediaID, MediaPath, MediaFile, MediaType from MultimediaTable;);
$sth = $dbh->prepare( $stmt );
my $rv = $sth->execute() or die $DBI::errstr;


if($rv < 0) {
   print $DBI::errstr;
}

my $row2_count = 0;
my $LinkID;

while(my @row = $sth->fetchrow_array()) {

   # print "MediaID = ". $row[0] . "\n";
   #   print "MediaPath = ". $row[1] ."\n";
   #   print "MediaFile = ". $row[2] ."\n";
   #   next;

   # if ($row[2] =~ / \([0-9,A-F]+\)\./) {   
   if ($row[2] =~ / /) {   
      printf("\n");
      print "MediaID = ". $row[0] . "\n";
      print "MediaPath = ". $row[1] ."\n";
      print "MediaFile = ". $row[2] ."\n";

      my $strMediaType = "";
      if ($row[3] eq 1) { $strMediaType = "Image"; }
      if ($row[3] eq 2) { $strMediaType = "File"; }
      if ($row[3] eq 3) { $strMediaType = "Sound"; }
      if ($row[3] eq 4) { $strMediaType = "Video"; }

      print "MediaType = ". $row[3] . " : $strMediaType\n";


      # what person is this MediaFile associated with?
     
      my $stmt = qq(SELECT LinkID, MediaID, OwnerType, OwnerID FROM MediaLinkTable WHERE MediaID = $row[0];); 
      printf("%s\n", $stmt);
      my $sth = $dbh->prepare( $stmt );
      my $rv = $sth->execute() or die $DBI::errstr;


      # How many rows where returned?
      $row2_count = 0;
      while(my @row2 = $sth->fetchrow_array()) {
         $row2_count++;
      }
      printf "This many rows where returned: %0d\n", $row2_count; 

      # if ($row2_count ne 1) {
      #    next;
      # }


      $sth = $dbh->prepare( $stmt );
      $rv = $sth->execute() or die $DBI::errstr;


      while(my @row2 = $sth->fetchrow_array()) {
         my $strOwnerType = "";
         if ($row2[2] eq 0)  { $strOwnerType = "Person"; }
         if ($row2[2] eq 1)  { $strOwnerType = "Family"; }
         if ($row2[2] eq 2)  { $strOwnerType = "Event"; }
         if ($row2[2] eq 3)  { $strOwnerType = "Source"; }
         if ($row2[2] eq 4)  { $strOwnerType = "Citation"; }
         if ($row2[2] eq 5)  { $strOwnerType = "Place"; }
         if ($row2[2] eq 6)  { $strOwnerType = "Task"; }
         if ($row2[2] eq 7)  { $strOwnerType = "Name (Primary or Alternate)"; }
         if ($row2[2] eq 14) { $strOwnerType = "Place Details"; }
         if ($row2[2] eq 19) { $strOwnerType = "Association"; }

         printf "   -------------------------------\n";
         $LinkID = $row2[0];
         print  "   LinkID    = ". $row2[0] . "\n";
         print  "   MediaID   = ". $row2[1] . "\n";
         print  "   OwnerType = ". $row2[2] . " : $strOwnerType\n";
         print  "   OwnerID   = ". $row2[3] . "\n";

         # If a person, get the name associated with this person
         if ($row2[2] eq 0) {
            my $stmt = qq(SELECT NameID, OwnerID, Surname, Given FROM NameTable WHERE OwnerID = $row2[3];); 
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
         if ($row2[2] eq 4) {
            my $stmt = qq(SELECT * FROM CitationTable WHERE CitationID = $row2[3];); 
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


         printf "   -------------------------------\n";

      }    
        


      if ($row[0] eq 308 || 
          $row[0] eq 338 || 
          $row[0] eq 459 || 
          $row[0] eq 694 || 
          $row[0] eq 899 || 
          $row[0] eq 1922 || 
          $row[0] eq 1944 || 
          $row[0] eq 2158 || 
          $row[0] eq 3251 || 
          $row[0] eq 3252 || 
          $row[0] eq 3857 || 
          $row[0] eq 3873 || 
          $row[0] eq 4082 || 
          $row[0] eq 4822 || 
          $row[0] eq 5177 || 
          $row[0] eq 5427 || 
          $row[0] eq 5436 || 
          $row[0] eq 5437 || 
          $row[0] eq 6293 || 
          $row[0] eq 7116 || 
          $row[0] eq 7117 || 
          $row[0] eq 1378 || 
          $row[0] eq 1754 || 
          $row[0] eq 1931 || 
          $row[0] eq 2235 || 
          $row[0] eq 2356 || 
          $row[0] eq 4026 || 
          $row[0] eq 4677 || 
          $row[0] eq 4678 || 
          $row[0] eq 5708 || 
          $row[0] eq 5832 || 
          $row[0] eq 5859 || 
          $row[0] eq 5860 || 
          $row[0] eq 5869 || 
          $row[0] eq 6084 || 
          $row[0] eq 6269 || 
          $row[0] eq 6356 || 
          $row[0] eq 6746 || 
          $row[0] eq 177 || 
          $row[0] eq 178 || 
          $row[0] eq 230 || 
          $row[0] eq 305 || 
          $row[0] eq 315 || 
          $row[0] eq 441 || 
          $row[0] eq 517 || 
          $row[0] eq 519 || 
          $row[0] eq 542 || 
          $row[0] eq 704 || 
          $row[0] eq 351) {
         printf("Skipping this record\n");
         next;
      }


      # if ($row[0] eq 6089) {
      #    printf "INFO: skipping this record\n";
      #    next;
      # }



      my $relpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $row[2]);
      # ensure file exists
      unless ( -r "$relpath" ) {
         printf "Error: MediaFile \"%s\" not readable, going to next match\n", $relpath;

         # if ($row2_count eq 1) {
         #    printf "INFO: delete medialink record %d and media record %d\n", $LinkID, $row[0];
         #    my $stmt = qq(DELETE FROM MediaLinkTable WHERE LinkID = $LinkID;); 
         #    printf("%s\n", $stmt);
         #    my $sth = $dbh->prepare( $stmt );
         #    my $rv = $sth->execute() or die $DBI::errstr;
         #    $stmt = qq(DELETE FROM MultimediaTable WHERE MediaID = $row[0];); 
         #    printf("%s\n", $stmt);
         #    $sth = $dbh->prepare( $stmt );
         #    $rv = $sth->execute() or die $DBI::errstr;
         # }

         next;
      }

      my $newMediaFile = $row[2];
      $newMediaFile =~ s/ \([0-9,A-F]+\)\././;
      $newMediaFile =~ s/\s\s*/_/g;
      $newMediaFile =~ s/,/_/g;
      $newMediaFile =~ s/\'//g;
      $newMediaFile =~ s/\(/_/g;
      $newMediaFile =~ s/\)/_/g;

      if ($newMediaFile =~ /\'/) {
         printf("File contains a single quote: \"%s\"", $newMediaFile);
         printf("\n");
         next;
      }

      if ($newMediaFile =~ /\"/) {
         printf("File contains a double quote: \"%s\"", $newMediaFile);
         printf("\n");
         next;
      }

      printf "newMediaFile = \"%s\"\n", $newMediaFile;

      my $newrelpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $newMediaFile);
      if ( -r "$newrelpath" ) {
         printf "Warning: 1st pass: New file already exists readable\n";

         $newMediaFile = sprintf("%s-%s", $row[0], $newMediaFile);
         $newrelpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $newMediaFile);
         if ( -r "$newrelpath" ) {
            printf "Error: 2nd pass: New file already exists readable\n";
            next;
         }
      }



      printf("rename file: \"%s\" to \"%s\"\n", $row[2], $newMediaFile);
      $newrelpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $newMediaFile);


      my $rv2;
      # rename file, if success, then update table entry
      my $rtn2 = rename($relpath, $newrelpath);

      if ($rtn2 eq 0) {
         printf("ERROR: could not rename \"%s\" to \"%s\"\n", $relpath, $newrelpath);
         printf("\n");
         next;

      } else {
         printf("INFO: Renamed \"%s\" to \"%s\"\n", $relpath, $newrelpath);

         # UPDATE MultiMediaTable 
         # SET MediaFile = (SELECT NewName FROM zMediaNewNames z WHERE MultimediaTable.MediaID = z.MediaID)
         # WHERE MediaID IN (SELECT DISTINCT MediaID FROM zMediaNewNames)

         my $stmt2 = sprintf("UPDATE MultiMediaTable SET MediaFile = '%s' WHERE MediaID = %s", $newMediaFile, $row[0]);
         printf "%s ;\n", $stmt2;


         $rv2 = $dbh->do($stmt2) or do { printf("\$DBI::errstr: %s\n", $DBI::errstr);
                                         printf("Rename file back to orig\n");
                                         rename($newrelpath, $relpath) or die "rename back did not work";
                                         die; };
         if( $rv2 < 0 ) {
            print $DBI::errstr;
            rename($newrelpath, $relpath) or die "rename back did not work";
            die;
         } else {
            print "Total number of rows updated : $rv2\n";
         }



      }
   }
}

print "Operation done successfully\n";

$dbh->disconnect();



exit(0);


sub reindex_rmnocase {
   my $stmt = qq(REINDEX RMNOCASE;);
   my $sth = $dbh->prepare( $stmt );
   my $rv = $sth->execute() or die $DBI::errstr;
   if($rv < 0) {
      print $DBI::errstr;
   }
}
