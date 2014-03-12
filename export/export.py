#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import csv
import ConfigParser
from sys import argv
import os
import datetime as dt

# Config file
Config = ConfigParser.ConfigParser()
Config.read('export.cfg')

def remove_oldfile():
   dirPath = "%s" % (ConfigSectionMap("log")['path'])
   fileList = os.listdir(dirPath)
   for fileName in fileList:
      os.remove(dirPath+"/"+fileName)

def ConfigSectionMap(section):
   """ ritorna il valore della sezione di configurazione
       specificata """
   dict1 = {}
   options = Config.options(section)
   for option in options:
      try:
         dict1[option] = Config.get(section, option)
         if dict1[option] == -1:
            DebugPrint("skip: %s" % option)
      except:
         print("exception on %s!" % option)
         dict1[option] = None
   return dict1

def export(db, table, movingavg, day, host):
   debug = int(ConfigSectionMap("debug")['debug'])
   """esporta i dati dal database secondo
   la media mobile selezionata"""
   mydb = MySQLdb.connect(host='%s' % (ConfigSectionMap("mysql")['host']),
       user='%s' % (ConfigSectionMap("mysql")['user']),
       passwd='%s' % (ConfigSectionMap("mysql")['password']),
       db='%s' % (db))
   cursor = mydb.cursor()
   if debug:
      print "[+] Aperta connessione mysql"

   if table is "ram":
      query_ram = "SELECT FLOOR( ROUND( UNIX_TIMESTAMP( timestamp ) / (%s*60) , 4)) AS timekey, timestamp, ROUND (AVG( kbmemused ) , 2) as memused, ROUND (AVG( kbbuffers ) , 2) as buffers, ROUND (AVG( commit ) , 2) as commit FROM ram where timestamp like '%s%s' GROUP BY timekey" % (movingavg, day, "%")
      cursor.execute(query_ram)
      result = cursor.fetchall()
      ramcsv = csv.writer(open("%s/ram_%s.csv" % (ConfigSectionMap("log")['path'], host), "a"), dialect='excel')
      for row in result:
         ramcsv.writerow(row)
      if debug:
         print "[+] Dati RAM esportati nel csv"

   if table is "cpu":
      query_cpu = "SELECT FLOOR( ROUND( UNIX_TIMESTAMP( timestamp ) / (%s*60) , 4)) AS timekey, timestamp, ROUND (AVG( user ) , 2) as user, ROUND (AVG( system ) , 2) as system, ROUND (AVG( idle ) , 2) as idle FROM cpu where timestamp like '%s%s' GROUP BY timekey" % (movingavg, day, "%")
      cursor.execute(query_cpu)
      result = cursor.fetchall()
      cpucsv = csv.writer(open("%s/cpu_%s.csv" % (ConfigSectionMap("log")['path'], host), "a"), dialect='excel')
      for row in result:
         cpucsv.writerow(row)
      if debug:
         print "[+] Dati CPU esportati nel csv"

   if table is "ldavg":
      query_ldavg = "SELECT FLOOR( ROUND( UNIX_TIMESTAMP( timestamp ) / (%s*60) , 4)) AS timekey, timestamp, ROUND (AVG( ldavg1 ) , 2) as ldavg1, ROUND (AVG( ldavg5 ) , 2) as ldavg5, ROUND (AVG( ldavg15 ) , 2) as ldavg15 FROM ldavg where timestamp like '%s%s' GROUP BY timekey" % (movingavg, day, "%")
      cursor.execute(query_ldavg)
      result = cursor.fetchall()
      ldavgcsv = csv.writer(open("%s/ldavg_%s.csv" % (ConfigSectionMap("log")['path'], host), "a"), dialect='excel')
      for row in result:
         ldavgcsv.writerow(row)
      if debug:
         print "[+] Dati LDAVG esportati nel csv"

   if table is "auth":
      query_auth = "SELECT FLOOR( ROUND( UNIX_TIMESTAMP( timestamp ) / (%s*60) , 4)) AS timekey, timestamp, ROUND (AVG( auth ) , 2) as auth FROM auth where timestamp like '%s%s' GROUP BY timekey" % (movingavg, day, "%")
      cursor.execute(query_auth)
      result = cursor.fetchall()
      authcsv = csv.writer(open("%s/cpu_%s.csv" % (ConfigSectionMap("log")['path'], host), "a"), dialect='excel')
      for row in result:
         authcsv.writerow(row)
      if debug:
         print "[+] Dati AUTH esportati nel csv"

   if table is "acct":
      query_acct = "SELECT FLOOR( ROUND( UNIX_TIMESTAMP( timestamp ) / (%s*60) , 4)) AS timekey, timestamp, ROUND (AVG( start ) , 2) as start, ROUND (AVG( stop ) , 2) as stop FROM acct where timestamp like '%s%s' GROUP BY timekey" % (movingavg, day, "%")
      cursor.execute(query_acct)
      result = cursor.fetchall()
      authcsv = csv.writer(open("%s/cpu_%s.csv" % (ConfigSectionMap("log")['path'], host), "a"), dialect='excel')
      for row in result:
         acctcsv.writerow(row)
      if debug:
         print "[+] Dati ACCT esportati nel csv"

   cursor.close()

def main():
   script, start, stop, movingavg = argv
   remove_oldfile()
   stop_list = stop.split('/')
   start_list = start.split('/')
   s = dt.datetime(int(start_list[0]),int(start_list[1]),int(start_list[2]))
   n = dt.datetime(int(stop_list[0]),int(stop_list[1]),int(stop_list[2]))
   for i in range((n - s).days + 1):
      day = (s+dt.timedelta(days = i)).date()
      print day
      for table in ["ram", "ldavg", "cpu", "swap", "auth", "acct"]:
         export(ConfigSectionMap("client1")['db'], table, movingavg, day, ConfigSectionMap("client1")['hostname'])
      for table in ["ram", "ldavg", "cpu", "swap", "auth", "acct"]:
         export(ConfigSectionMap("client2")['db'], table, movingavg, day, ConfigSectionMap("client2")['hostname'])

if __name__ == "__main__":
   main()
