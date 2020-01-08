include Java
import java.io.IOException
import org.apache.hadoop.hbase.PleaseHoldException
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.commons.logging.LogFactory

require 'optparse'

usage = 'Usage : ./hbase org.jruby.Main restore_snapshot.rb <table_name> <snapshot_date>\n'
OptionParser.new do |o|
    o.banner = usage
    o.on('-h', '--help', 'Display help message') { puts o; puts; exit }
    o.parse!
end

if ARGV.empty?
    puts usage
    exit 1
end

table_name = ARGV.shift
snapshot_date = ARGV.shift

if table_name.nil? or snapshot_date.nil?
    puts usage
    exit 1
end

config = HBaseConfiguration.create
config.set 'fs.defaultFS', config.get(HConstants::HBASE_DIR)
admin = nil
while true
    begin
        admin = HBaseAdmin.new config
        break
        rescue IOException => _
        puts 'Waiting for master to start...\n'
        sleep 10
    end
end

puts 'listing tables'
while true
    begin
        admin.list_tables
        break
        rescue IOException => _
        puts 'Waiting for master to init...\n'
        sleep 10
    end
end

puts admin.list_tables
puts 'listing snapshots'
puts admin.list_snapshots

def restore_snapshot(admin, table_name, snapshot_date)
    if admin.tableExists table_name and admin.is_table_enabled table_name
        puts 'disabling table ' + table_name
        admin.disable_table table_name
    end

    puts 'restoring snapshot ' + table_name + '-' + snapshot_date
    admin.restore_snapshot(table_name + '-' + snapshot_date)

    if admin.is_table_disabled table_name
        puts 'enabling table ' + table_name
        admin.enable_table table_name
    end
end

restore_snapshot(admin, table_name, snapshot_date)
