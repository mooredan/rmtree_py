#!/usr/bin/perl -w

use DBI;
use strict;

my $dry_run = 1;

my $sth;
my $driver   = "SQLite"; 
# my $database = "test.db";
my $database = "../ZebMoore_Ancestry.rmtree";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1, sqlite_unicode => 1 }) 
   or die $DBI::errstr;


$dbh->sqlite_enable_load_extension(1);
print "Loading extension\n";
$sth = $dbh->prepare("select load_extension('./unifuzz.so')")
    or die "Cannot prepare: " . $dbh->errstr();
my $rtn = $sth->execute() or die $DBI::errstr;
print "Extension loaded\n";


print "Opened database $database successfully\n";

my $stmt = qq(SELECT MediaID, MediaPath, MediaFile from MultimediaTable;);
$sth = $dbh->prepare( $stmt );
my $rv = $sth->execute() or die $DBI::errstr;


if($rv < 0) {
   print $DBI::errstr;
}

while(my @row = $sth->fetchrow_array()) {

   if ($row[2] =~ / \([0-9,A-F]+\)\./) {   
      printf("\n");
      print "MediaID = ". $row[0] . "\n";
      print "MediaPath = ". $row[1] ."\n";
      print "MediaFile = ". $row[2] ."\n";


      my $relpath = sprintf("%s/%s", "../ZebMoore_Ancestry_media", $row[2]);
      # ensure file exists
      unless ( -r "$relpath" ) {
         printf "Error: MediaFile not readable, going to next match\n";
         next;
      }

      my $newMediaFile = $row[2];
      $newMediaFile =~ s/ \([0-9,A-F]+\)\././;
      $newMediaFile =~ s/\s\s*/_/g;
      $newMediaFile =~ s/,/_/g;
      $newMediaFile =~ s/\'//g;

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
         printf "Error: 1st pass: New file already exists readable\n";

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
      unless(rename($relpath, $newrelpath)) {

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
