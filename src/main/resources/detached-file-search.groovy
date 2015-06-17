import io.dbmaster.tools.wmi.WMIQuery
import io.dbmaster.tools.wmi.WMIQuery.WMIQueryResult
import com.branegy.service.core.QueryRequest
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JDBCDialect
import com.branegy.service.connection.api.ConnectionService
import groovy.sql.Sql

import java.sql.Statement
import java.sql.ResultSet


public String toSize(Number bytes) {
    def fileSizeUnits = [ "bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB" ]

    if (bytes==null) {
        return "---"
    } else {
        double bytes2 = bytes.doubleValue()
        String sizeToReturn = ""
        int index = 0
        for(index = 0; index < fileSizeUnits.size(); index++) {
            if(bytes2 < 1024) {
                break
            }
            bytes2 = bytes2 / 1024
        }
        sizeToReturn = String.format('%1$,.2f', bytes2) + " " + fileSizeUnits[index]
        return sizeToReturn
    }
}

public String toDate(String dateIn) {
    def parserIn =new java.text.SimpleDateFormat("yyyyMMddHHmmss")
    def parserOut =new java.text.SimpleDateFormat("yyyy/MM/dd")
    def date = parserIn.parse(dateIn.substring(0,16))
    return parserOut.format(date)
}
    
    

def runQuery = { host, namespace, query, file2db, foldersToExclude ->
    logger.info("Querying \\\\${host}\\${namespace}")
    WMIQueryResult result = WMIQuery.execute(host, namespace, query)
    logger.debug("Query completed")
    println """<h3>Results for ${host}</h3><table cellspacing="0" class="simple-table" border="1">
               <tr style="background-color:#EEE"><th>Database</th>"""

    def nameIndex = -1;
    result.headers.eachWithIndex { h,i -> 
        println "<th>${h}</th>" 
        if (h.equals("Name")) {
            nameIndex = i;
        }
    }
    logger.debug("NameIndex = ${nameIndex}")
    println "</tr>"
 /*   
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    
    String text = date.toString(formatter);
    LocalDate date = LocalDate.parse(text, formatter);
    
    // yyyymmddhhnnss.zzzzzzsUUU
 */
 
    result.values.each { row ->
        def fileName = row[nameIndex].toUpperCase()
        def exlcude = foldersToExclude.find { it.length() > 0 && fileName.startsWith(it) }
        if (exlcude==null) {
            println "<tr>"
            def dbName  = file2db[fileName]
            if (dbName==null) { 
                print "<td><strong>detached</strong</td>"
            } else {
                print "<td>${dbName}</td>"        
            }
            
            def CreationDate = row[0]
            def FileSize = new Long(row[1])
            def LastAccessed = row[2]
            def LastModified = row[3]
            def Name = row[4]

            //for (int i=0; i<row.length; i++) {
              //  if (row[i]!=null) {
                    // logger.debug("class for ${i}="+row[i].getClass().name);
                //}
                // println "<td>${row[i]}</td>"
            //}
            print "<td>${toDate(CreationDate)}</td>"
            print "<td align=\"right\">${toSize(FileSize)}</td>"
            print "<td>${toDate(LastAccessed)}</td>"
            print "<td>${toDate(LastModified)}</td>"
            print "<td>${Name}</td>"
            println "</tr>"
        } else {
            logger.debug("Excluding file ${fileName}")
        }
    }
    println "</table>"
}

ConnectionService connectionSrv = dbm.getService(ConnectionService.class)
def connections
if (p_servers!=null && p_servers.size()>0) {
    connections = p_servers.collect { serverName -> connectionSrv.findByName(serverName) }
} else {
    connections  = connectionSrv.getConnectionList()
}

def foldersToExclude
if (p_exclude_folders!=null) {
    foldersToExclude = p_exclude_folders.split("\\r?\\n").collect { it.trim().toUpperCase() }
} else {
    foldersToExclude = []
}
   
def processedHosts = []
def namespace = "root\\cimv2"

/*
def wmiQuery = """
 select AccessMask, 
        Name, 
        Caption, 
        CreationDate, 
        Drive, FileName, FileSize, LastAccessed, LastModified 
   from CIM_DataFile 
  where (Extension='ldf' or Extension='ndf' or Extension='mdf') 
"""
*/

// TODO FileSize is in Bytes - change to Gb / Mb ...
// TODO Fix issue when more than one instnace installed at the same host
// TODO Add host as a column
// TODO Improve dates layout (only date will be enough)
// TODO Sort by database name 

def wmiQuery = """
 select Name, 
        FileSize, 
        CreationDate, 
        LastAccessed, 
        LastModified 
   from CIM_DataFile 
  where (Extension='ldf' or Extension='ndf' or Extension='mdf') 
"""
// and (not  name like "c:\\windows\\winsxs%")

connections.each { connection ->
    try {
        def connector = ConnectionProvider.getConnector(connection)
        def dialect = connector.connect()
  
        if (!(dialect instanceof JDBCDialect) || !((JDBCDialect)dialect).getDialectName().contains("sqlserver")) {
            logger.info("Skipping checks for connection ${connection.getName()} as it is not a database one")
            return
        } else {
            logger.info("Connecting to ${connection.getName()}")
        }
        def jdbcConnection = connector.getJdbcConnection(null)
        dbm.closeResourceOnExit(jdbcConnection)
    
        Statement statement = jdbcConnection.createStatement();
        ResultSet rs = statement.executeQuery("select SERVERPROPERTY('MachineName')")
        def file2db = [:]
        if (rs.next()) {
            String machineName = rs.getString(1)
            if (processedHosts.contains(machineName)) {
                logger.warn("Skipping wmi query for ${connection.getName()}: host ${machineName} already checked")
            } else {
            
                def sql_query = "select db_name(database_id) as db, physical_name from sys.master_files"
                def sql = new Sql(jdbcConnection)
                sql.eachRow(sql_query){ row ->
                    file2db[row.physical_name.toUpperCase()] = row.db
                }
                logger.info("Running query for host ${machineName}")
                processedHosts.add(machineName)
                
                runQuery(machineName, namespace, wmiQuery, file2db, foldersToExclude)
            }
        } else {
            logger.warn("Cannot get machine name for connection ${connection.getName()}")
        }
        jdbcConnection.close()            
    } catch (Exception e) {
        logger.error("Cannot run query for ${connection.getName()}",e)
    }
}