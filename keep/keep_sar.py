#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import datetime
import MySQLdb
import ConfigParser
import gzip

# Read config file
Config = ConfigParser.ConfigParser()
Config.read('keep.cfg')


def ConfigSectionMap(section):
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


def yesterday():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(1)
    return yesterday


def import_db(db, cpu, ram, swap, ldavg):
    debug = int(ConfigSectionMap("debug")['debug'])
    mydb = MySQLdb.connect(host='%s' % (ConfigSectionMap("mysql")['host']),
    user='%s' % (ConfigSectionMap("mysql")['user']),
    passwd='%s' % (ConfigSectionMap("mysql")['password']),
    db='%s' % (db))
    cursor = mydb.cursor()
    if debug:
        print "[+] Aperta connessione mysql"

    try:
        cpu_data = csv.reader(gzip.open(cpu), delimiter=';')
        for row in cpu_data:
           cursor.execute('INSERT INTO cpu(timestamp, \
              user, system, idle)' \
              'VALUES(%s, %s, %s, %s)',
              row)
        mydb.commit()
        if debug:
           print "[+] Dati CPU importati nel database"
    except IOError:
      if debug:
          print "[-] Il file %s non e' presente" % cpu

    try:
       ram_data = csv.reader(gzip.open(ram), delimiter=';')
       for row in ram_data:
          cursor.execute('INSERT INTO ram(timestamp, \
             kbmemused, kbbuffers, commit)' \
             'VALUES(%s, %s, %s, %s)',
             row)
       mydb.commit()
       if debug:
          print "[+] Dati RAM importati nel database"
    except IOError:
       if debug:
          print "[-] Il file %s non e' presente" % ram

    try:
       swap_data = csv.reader(gzip.open(swap), delimiter=';')
       for row in swap_data:
          cursor.execute('INSERT INTO swap(timestamp, \
             kbswpfree, swpused)' \
             'VALUES(%s, %s, %s)',
             row)
       mydb.commit()
       if debug:
          print "[+] Dati SWAP importati nel database"
    except IOError:
       if debug:
          print "[-] Il file %s non e' presente" % swap

    try:
       ldavg_data = csv.reader(gzip.open(ldavg), delimiter=';')
       for row in ldavg_data:
          cursor.execute('INSERT INTO ldavg(timestamp, \
             ldavg1, ldavg5, ldavg15)' \
             'VALUES(%s, %s, %s, %s)',
             row)
       mydb.commit()
       if debug:
          print "[+] Dati Load-Average importati nel database"
    except IOError:
       if debug:
          print "[-] Il file %s non e' presente" % ldavg

    cursor.close()


def wrapper_db(db, host):
   ram = '%s/ram-%s_%s.csv.gz' % (ConfigSectionMap("log")['path'], yesterday(), host)
   ldavg = '%s/load-average-%s_%s.csv.gz' % (ConfigSectionMap("log")['path'], yesterday(), host)
   cpu = '%s/cpu-%s_%s.csv.gz' % (ConfigSectionMap("log")['path'], yesterday(), host)
   swap = '%s/swap-%s_%s.csv.gz' % (ConfigSectionMap("log")['path'], yesterday(), host)
   import_db(db, cpu, ram, swap, ldavg)


def main():
   debug = int(ConfigSectionMap("debug")['debug'])
   if debug:
      print "[+] Importazione dei dati dell'host: %s" % ConfigSectionMap("client1")['hostname']
   wrapper_db(ConfigSectionMap("client1")['db'], ConfigSectionMap("client1")['hostname'])
   if debug:
      print "[+] Importazione dei dati dell'host %s" % ConfigSectionMap("client2")['hostname']
   wrapper_db(ConfigSectionMap("client2")['db'], ConfigSectionMap("client2")['hostname'])


if __name__ == "__main__":
   main()
