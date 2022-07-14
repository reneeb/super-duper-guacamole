#!/usr/bin/perl

use v5.24;

use strict;
use warnings;

use Data::Printer;
use DBI;
use JSON;
use Getopt::Long;
use URI;
use Time::Piece;
use Time::Seconds;
use List::Util qw(shuffle);
use Data::UUID;

use experimental 'signatures';

GetOptions(
    'config=s' => \my $config_path,
	'out=s'    => \my $out_path,
	'urls=s'   => \my $url_file,
);

if ( !$out_path || !-d $out_path ) {
	die "Output path $out_path does not exist!";
}

if ( !$config_path || !-f $config_path ) {
	die "Config file $config_path does not exist!";
}

my $dbh    = _prepare_history_db( $out_path );
my $config = _get_config( $config_path );
my $urls   = _get_urllist( $url_file );
_fake_history( $dbh, $config, $urls );

sub _fake_history ( $db, $config, $urls ) {
	
	my @request_timestamps = _get_timestamps( $config, $urls );
	
	my %url_map = _save_urls( $db, \@request_timestamps );
	_save_visits( $db, \@request_timestamps, \%url_map );
	_save_downloads( $db, \@request_timestamps, \%url_map );
}

sub _save_urls ( $db, $requests ) {
	
	my %counts;
	for my $request ( $requests->@* ) {
		my $url = $request->{request}->{URL};
		$counts{$url}->{count}++;
		$counts{$url}->{last_visit} = $request->{date};
	}
	
	my %map;
	
	my $sql = q~
	    INSERT INTO "urls" 
		    ("id","url","title","visit_count","typed_count","last_visit_time","hidden") 
		VALUES (?, ?, ?, ?, ?, ?, ?)
	~;
	
	my $sth = $db->prepare( $sql );
	
	my %seen;
	
	my $cnt = 1;
	for my $request ( $requests->@* ) {
		my $info = $request->{request};
		my $url  = $info->{URL};
		
		next if $seen{$url}++;
		
		$map{$url} = $cnt;
		
		$sth->execute( $cnt++, $url, $info->{Title} || 'Home', $counts{$url}->{count}, 0, $counts{$url}->{last_visit}, 0 );
	}
	
	return %map;
}

sub _save_visits ( $db, $requests, $urls ) {
	
	my $sql = q~
	    INSERT INTO "visits" 
		    ("id","url","visit_time","from_visit","transition","segment_id","visit_duration","incremented_omnibox_typed_score","opener_visit")
		VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? )
	~;
	
	my $sth = $db->prepare( $sql );
	
	my $sql_keywords = q~
	    INSERT INTO "keyword_search_terms" 
		    ("keyword_id","url_id","term","normalized_term")
		VALUES (?,?,?,?);
	~;
	
	my $sth_keywords = $db->prepare( $sql_keywords );
	
	my $cnt = 1;
	my $cnt_keyword = 1;
	
	REQUEST:
	for my $request ( $requests->@* ) {
		my $info = $request->{request};
		my $url  = $info->{URL};
		
		$sth->execute( $cnt++, $urls->{$url}, $request->{date}, 0, 805306368, 0, int( rand 1_000 ), 0, 0 );
		
		if ( $url =~ m{google} ) {
			my ($search_term)   = $url =~ m{q=(.+)};
			
			next REQUEST if !$search_term;
			
			my $normalized_term = $search_term =~ s{\+}{ }gr;
			
			$sth_keywords->execute( $cnt_keyword++, $urls->{$url}, $search_term, $normalized_term );
		}
	}
}

sub _save_downloads ( $db, $requests, $urls ) {
	
	my $sql = q~
	    INSERT INTO "downloads" 
		    ("id", "guid", 	"current_path", "target_path", "start_time", "received_bytes", "total_bytes",
			"state", "danger_type", "interrupt_reason", "hash", "end_time", "opened", "last_access_time",
			"transient", "referrer", "site_url", "embedder_download_data", "tab_url", "tab_referrer_url",
			"http_method", "by_ext_id", "by_ext_name", "etag", "last_modified", "mime_type", "original_mime_type")
		VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
	~;
	
	my $sth = $db->prepare( $sql );
	
	my $sql_chain = q~
	    INSERT INTO "downloads_url_chains" 
		    ("id", "chain_index", "url")
		VALUES ( ?, ?, ?)
	~;
	
	my $sth_chain = $db->prepare( $sql_chain );
	
	my $cnt = 1;
	
	DOWNLOAD:
	for my $request ( $requests->@* ) {
		my $info = $request->{request};
		my $url  = $info->{URL};
		
		next DOWNLOAD if !$info->{Download};
		
		my $uuid = Data::UUID->new->create_str;
		my $path = sprintf 'C:\Users\Heinz Werner\Downloads\%s', $info->{Download};
		
		my $uri       = URI->new( $url );
		my $referrer  = sprintf '%s://%s', $uri->scheme, $uri->host;
		my $mime_type = $info->{MimeType} || 'application/octet-stream';
		
		$sth_chain->execute( $cnt, 0, $url );
		
		$sth->execute(
		    $cnt++, $uuid, $path, $path, $request->{date}, $info->{Bytes}, $info->{Bytes},
			1, 0, 0, '', $request->{date} + ( 100000 * 60 * int rand 20 ), 0, 0,
			0, $referrer, '', '', $url, $referrer,
			'', '', '', '', '', $mime_type, $mime_type
		);
	}
	
}

sub _get_timestamps ( $config, $urls ) {
	my $start = Time::Piece->strptime( $config->{Start}, "%Y-%m-%d");
	my $end   = Time::Piece->strptime( $config->{End}, "%Y-%m-%d");
	
	my %day_requests;
	for my $url ( $config->{URLs}->@* )  {
		my $local_date = Time::Piece->strptime( $url->{Date}, "%Y-%m-%d" );
		
		push $day_requests{$local_date->ymd}->@* , $url;
	}
	
	my @request_timestamps;
	
	while ( $start < $end ) {
		my $count_requests = int rand 60;
		my $key            = $start->ymd;
		
		my $hour   = int rand 22;
		my $minute = int rand 59;
		my $second = 0;
			
		my $uri = URI->new( ( shuffle $urls->@* )[0] || 'google.com' );
		
		say sprintf "%s %02d:%02d:%02d -> %s", $start->ymd, $hour, $minute, $second % 60, $uri;
		
		for ( 0 .. $count_requests ) {
		    my $request_date = sprintf "%s %02d:%02d:%02d", $start->ymd, $hour, $minute, $second % 60;
			my $epoche       = _date2epoch( $request_date );
			
		    push @request_timestamps, {
				date => $epoche,
				request => {
					URL => $uri,
				},
			};
			
		    $second += 2;
	    }
		
		for my $day_request ( @{ $day_requests{$key} || [] } ) {
		    my $request_date = sprintf "%s %02d:%02d:%02d", $start->ymd, $hour, $minute, $second % 60;
			my $epoche       = _date2epoch( $request_date );
			
		    push @request_timestamps, {
				date    => $epoche,
				request => $day_request,
			};
			
			$second += 2;
		}
		
		$start += ONE_DAY;
	}
	
	return @request_timestamps;
}

sub _date2epoch ( $date ) {
	my $unix_epoch = Time::Piece->strptime( $date, "%Y-%m-%d %H:%M:%S" )->epoch;  # starts from 1970-01-01
	my $win_epoch  = $unix_epoch + 11644473600;                                   # windows epoch starts from 1601-01-01
	return $win_epoch * 1_000_000;
}

sub _get_urllist ( $file ) {
	return if !-f $file;
	
	my $content = do { local (@ARGV, $/) = $file; <> };
	
	my @urls = map { ( split /\s+/, $_ )[1] } split /\n/, $content;
	return \@urls;
}

sub _get_config ( $file ) {
	my $content = do { local (@ARGV, $/) = $file; <> };
	die "Config file was empty" if !$content;
	
	my $config;
	eval {
		$config = JSON->new->utf8(1)->decode( $content );
	};
	
	die "No valid JSON in config file: $@" if !$config;
	
	return $config;
}

sub _prepare_history_db ( $out ) {
	my $db = "$out/HISTORY";
	
	if ( -f $db ) {
		unlink $db;
	}
	
	my $dbh = DBI->connect( "DBI:SQLite:$db" ) or die $DBI::errstr;
	
	local $/;
	
	my @stmts = split /;\n/, <DATA>;
	$dbh->do( $_ ) for @stmts;
	
	return $dbh;
}


__DATA__
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "meta" (
	"key"	LONGVARCHAR NOT NULL UNIQUE,
	"value"	LONGVARCHAR,
	PRIMARY KEY("key")
);
CREATE TABLE IF NOT EXISTS "urls" (
	"id"	INTEGER,
	"url"	LONGVARCHAR,
	"title"	LONGVARCHAR,
	"visit_count"	INTEGER NOT NULL DEFAULT 0,
	"typed_count"	INTEGER NOT NULL DEFAULT 0,
	"last_visit_time"	INTEGER NOT NULL,
	"hidden"	INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "visits" (
	"id"	INTEGER,
	"url"	INTEGER NOT NULL,
	"visit_time"	INTEGER NOT NULL,
	"from_visit"	INTEGER,
	"transition"	INTEGER NOT NULL DEFAULT 0,
	"segment_id"	INTEGER,
	"visit_duration"	INTEGER NOT NULL DEFAULT 0,
	"incremented_omnibox_typed_score"	BOOLEAN NOT NULL DEFAULT FALSE,
	"opener_visit"	INTEGER,
	PRIMARY KEY("id")
);
CREATE TABLE IF NOT EXISTS "visit_source" (
	"id"	INTEGER,
	"source"	INTEGER NOT NULL,
	PRIMARY KEY("id")
);
CREATE TABLE IF NOT EXISTS "keyword_search_terms" (
	"keyword_id"	INTEGER NOT NULL,
	"url_id"	INTEGER NOT NULL,
	"term"	LONGVARCHAR NOT NULL,
	"normalized_term"	LONGVARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS "downloads" (
	"id"	INTEGER,
	"guid"	VARCHAR NOT NULL,
	"current_path"	LONGVARCHAR NOT NULL,
	"target_path"	LONGVARCHAR NOT NULL,
	"start_time"	INTEGER NOT NULL,
	"received_bytes"	INTEGER NOT NULL,
	"total_bytes"	INTEGER NOT NULL,
	"state"	INTEGER NOT NULL,
	"danger_type"	INTEGER NOT NULL,
	"interrupt_reason"	INTEGER NOT NULL,
	"hash"	BLOB NOT NULL,
	"end_time"	INTEGER NOT NULL,
	"opened"	INTEGER NOT NULL,
	"last_access_time"	INTEGER NOT NULL,
	"transient"	INTEGER NOT NULL,
	"referrer"	VARCHAR NOT NULL,
	"site_url"	VARCHAR NOT NULL,
	"embedder_download_data"	VARCHAR NOT NULL,
	"tab_url"	VARCHAR NOT NULL,
	"tab_referrer_url"	VARCHAR NOT NULL,
	"http_method"	VARCHAR NOT NULL,
	"by_ext_id"	VARCHAR NOT NULL,
	"by_ext_name"	VARCHAR NOT NULL,
	"etag"	VARCHAR NOT NULL,
	"last_modified"	VARCHAR NOT NULL,
	"mime_type"	VARCHAR(255) NOT NULL,
	"original_mime_type"	VARCHAR(255) NOT NULL,
	PRIMARY KEY("id")
);
CREATE TABLE IF NOT EXISTS "downloads_url_chains" (
	"id"	INTEGER NOT NULL,
	"chain_index"	INTEGER NOT NULL,
	"url"	LONGVARCHAR NOT NULL,
	PRIMARY KEY("id","chain_index")
);
CREATE TABLE IF NOT EXISTS "downloads_slices" (
	"download_id"	INTEGER NOT NULL,
	"offset"	INTEGER NOT NULL,
	"received_bytes"	INTEGER NOT NULL,
	"finished"	INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY("download_id","offset")
);
CREATE TABLE IF NOT EXISTS "downloads_reroute_info" (
	"download_id"	INTEGER NOT NULL,
	"reroute_info_serialized"	VARCHAR NOT NULL,
	PRIMARY KEY("download_id")
);
CREATE TABLE IF NOT EXISTS "segments" (
	"id"	INTEGER,
	"name"	VARCHAR,
	"url_id"	INTEGER NON,
	PRIMARY KEY("id")
);
CREATE TABLE IF NOT EXISTS "segment_usage" (
	"id"	INTEGER,
	"segment_id"	INTEGER NOT NULL,
	"time_slot"	INTEGER NOT NULL,
	"visit_count"	INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY("id")
);
CREATE TABLE IF NOT EXISTS "typed_url_sync_metadata" (
	"storage_key"	INTEGER NOT NULL,
	"value"	BLOB,
	PRIMARY KEY("storage_key")
);
CREATE TABLE IF NOT EXISTS "content_annotations" (
	"visit_id"	INTEGER,
	"visibility_score"	NUMERIC,
	"floc_protected_score"	NUMERIC,
	"categories"	VARCHAR,
	"page_topics_model_version"	INTEGER,
	"annotation_flags"	INTEGER NOT NULL,
	"entities"	VARCHAR,
	"related_searches"	VARCHAR,
	"search_normalized_url"	VARCHAR,
	"search_terms"	LONGVARCHAR,
	PRIMARY KEY("visit_id")
);
CREATE TABLE IF NOT EXISTS "context_annotations" (
	"visit_id"	INTEGER,
	"context_annotation_flags"	INTEGER NOT NULL,
	"duration_since_last_visit"	INTEGER,
	"page_end_reason"	INTEGER,
	"total_foreground_duration"	INTEGER,
	PRIMARY KEY("visit_id")
);
CREATE TABLE IF NOT EXISTS "clusters" (
	"cluster_id"	INTEGER,
	"score"	NUMERIC NOT NULL,
	PRIMARY KEY("cluster_id")
);
CREATE TABLE IF NOT EXISTS "clusters_and_visits" (
	"cluster_id"	INTEGER NOT NULL,
	"visit_id"	INTEGER NOT NULL,
	"score"	NUMERIC NOT NULL,
	PRIMARY KEY("cluster_id","visit_id")
) WITHOUT ROWID;
INSERT INTO "meta" ("key","value") VALUES ('mmap_status','-1');
INSERT INTO "meta" ("key","value") VALUES ('version','53');
INSERT INTO "meta" ("key","value") VALUES ('last_compatible_version','16');
INSERT INTO "meta" ("key","value") VALUES ('early_expiration_threshold','13293031007861361');;
INSERT INTO "segments" ("id","name","url_id") VALUES (2,'http://maps.google.de/',7);
INSERT INTO "segments" ("id","name","url_id") VALUES (3,'http://amazon.de/',14);
INSERT INTO "segments" ("id","name","url_id") VALUES (4,'http://obi.de/',16);
INSERT INTO "segments" ("id","name","url_id") VALUES (5,'http://tagx.de/',26);
INSERT INTO "segments" ("id","name","url_id") VALUES (6,'http://dayx.de/',28);
INSERT INTO "segment_usage" ("id","segment_id","time_slot","visit_count") VALUES (2,2,13300408800000000,1);
INSERT INTO "segment_usage" ("id","segment_id","time_slot","visit_count") VALUES (3,3,13300408800000000,3);
INSERT INTO "segment_usage" ("id","segment_id","time_slot","visit_count") VALUES (4,4,13300408800000000,1);
INSERT INTO "segment_usage" ("id","segment_id","time_slot","visit_count") VALUES (5,5,13300408800000000,1);
INSERT INTO "segment_usage" ("id","segment_id","time_slot","visit_count") VALUES (6,6,13300408800000000,1);
CREATE INDEX IF NOT EXISTS "visits_url_index" ON "visits" (
	"url"
);
CREATE INDEX IF NOT EXISTS "visits_from_index" ON "visits" (
	"from_visit"
);
CREATE INDEX IF NOT EXISTS "visits_time_index" ON "visits" (
	"visit_time"
);
CREATE INDEX IF NOT EXISTS "keyword_search_terms_index1" ON "keyword_search_terms" (
	"keyword_id",
	"normalized_term"
);
CREATE INDEX IF NOT EXISTS "keyword_search_terms_index2" ON "keyword_search_terms" (
	"url_id"
);
CREATE INDEX IF NOT EXISTS "keyword_search_terms_index3" ON "keyword_search_terms" (
	"term"
);
CREATE INDEX IF NOT EXISTS "segments_name" ON "segments" (
	"name"
);
CREATE INDEX IF NOT EXISTS "segments_url_id" ON "segments" (
	"url_id"
);
CREATE INDEX IF NOT EXISTS "segment_usage_time_slot_segment_id" ON "segment_usage" (
	"time_slot",
	"segment_id"
);
CREATE INDEX IF NOT EXISTS "segments_usage_seg_id" ON "segment_usage" (
	"segment_id"
);
CREATE INDEX IF NOT EXISTS "clusters_for_visit" ON "clusters_and_visits" (
	"visit_id"
);
CREATE INDEX IF NOT EXISTS "urls_url_index" ON "urls" (
	"url"
);
COMMIT;
