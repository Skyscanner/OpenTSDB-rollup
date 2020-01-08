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

usage = 'Usage : ./hbase org.jruby.Main wait_for_hbase_init.rb\n'
OptionParser.new do |o|
    o.banner = usage
    o.on('-h', '--help', 'Display help message') { puts o; puts; exit }
    o.parse!
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
