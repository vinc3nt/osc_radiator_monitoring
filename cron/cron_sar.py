#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import os
import csv
import sys
import datetime
import tempfile
import fileinput
import shutil
import gzip
import ConfigParser

# Config file
Config = ConfigParser.ConfigParser()
Config.read('cron.cfg')

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

def set_tmp_path(debug):
   global tmppath
   tmppath = tempfile.mkdtemp()
   if debug:
      print "[+] creata directory: " + tmppath
   return tmppath

def check_data_path(debug):
   directory = ConfigSectionMap("export")['directory']
   if not os.path.exists(directory):
      os.makedirs(directory)
      if debug:
         print "[+] Creata directory"

def yesterday():
   today = datetime.date.today()
   yesterday = today - datetime.timedelta(1)
   return yesterday

def set_weekday(yesterday, debug):
   numdaycheck=yesterday.strftime("%u")
   if len(numdaycheck) is 1:
      numdaycheck = "0"+str(numdaycheck)
      if debug:
         print "[+] analizzo i dati del giorno " + numdaycheck
      return numdaycheck
   if len(numdaycheck) is 2:
      if debug:
         print "[+] analizzo i dati del giorno " + numdaycheck
      return numdaycheck

def set_monthday(yesterday, debug):
   global monthday
   monthday = str(yesterday)[-2:]
   if debug:
      print "[+] analizzo i dati del giorno " + monthday
   return monthday

def remove_utc(csv,searchExp,replaceExp, debug):
   for line in fileinput.input(csv, inplace=1):
      if searchExp in line:
         line = line.replace(searchExp,replaceExp)
      sys.stdout.write(line)
   if debug:
      print "[+] rimosso UTC dal timestamp dal csv"

def remove_header(csv, debug):
   lines = open(csv).readlines()
   open(csv, 'w').writelines(lines[1:])
   if debug:
      print "[+] rimosso l'header dal csv"

def sar_to_csv(monthday, export, check, debug):
   os.system('sadf -d %s%s -t -- -%s > %s/%s.csv.tmp' % (ConfigSectionMap("log")['sar'], monthday, export, tmppath, check))
   csvpath = '%s/%s.csv.tmp' % (tmppath, check)
   remove_utc(csvpath," UTC","", debug)
   remove_header(csvpath, debug)
   if debug:
      print "[+] creato il csv: " + csvpath
   return csvpath

def custom_csv(monthday, export, debug):
   """Il modulo estrae dal csv solo le informazioni necessarie"""
   global check
   if export == "r":
      check = "ram"
      sarcsv = sar_to_csv(monthday, export, check, debug)
      try:
         with open(sarcsv, 'rb') as f:
            reader = csv.reader(f, delimiter=';')
            mycsv = '%s/%s-%s_%s.csv.gz' % (tmppath, check, yesterday(), ConfigSectionMap("client")['hostname'])
            csv_custom_path['ram'] = mycsv
            f = gzip.open(mycsv,'w')
            for row in reader:
               f.write(row[2]+";"+row[4]+";"+row[6]+";"+row[9]+"\n")
            f.close()
         if debug:
            print "[+] Custom check RAM eseguito"
      except IndexError:
         print "[-] Il sistema ha subito un riavvio, dati RAM parzialmente corrotti"

   if export == "u":
      check = "cpu"
      sarcsv = sar_to_csv(monthday, export, check, debug)
      try:
         with open(sarcsv, 'rb') as f:
            reader = csv.reader(f, delimiter=';')
            mycsv = '%s/%s-%s_%s.csv.gz' % (tmppath, check, yesterday(), ConfigSectionMap("client")['hostname'])
            csv_custom_path['cpu'] = mycsv
            f = gzip.open(mycsv,'w')
            for row in reader:
               f.write(row[2]+";"+row[4]+";"+row[6]+";"+row[9]+"\n")
            f.close()
         if debug:
            print "[+] Custom check CPU eseguito"
      except IndexError:
         print "[-] Il sistema ha subito un riavvio, dati CPU parzialmente corrotti"
      return 1

   if export == "q":
      check = "load-average"
      sarcsv = sar_to_csv(monthday, export, check, debug)
      try:
         with open(sarcsv, 'rb') as f:
            reader = csv.reader(f, delimiter=';')
            mycsv = '%s/%s-%s_%s.csv.gz' % (tmppath, check, yesterday(), ConfigSectionMap("client")['hostname'])
            csv_custom_path['ldavg'] = mycsv
            f = gzip.open(mycsv,'w')
            for row in reader:
               f.write(row[2]+";"+row[5]+";"+row[6]+";"+row[7]+"\n")
            f.close()
         if debug:
            print "[+] Custom check Load Average eseguito"
      except IndexError:
         print "[-] Il sistema ha subito un riavvio, dati LA parzialmente corrotti"
      return 1

   if export == "S":
      check = "swap"
      sarcsv = sar_to_csv(monthday, export, check, debug)
      try:
         with open(sarcsv, 'rb') as f:
            reader = csv.reader(f, delimiter=';')
            mycsv = '%s/%s-%s_%s.csv.gz' % (tmppath, check, yesterday(), ConfigSectionMap("client")['hostname'])
            csv_custom_path['swap'] = mycsv
            f = gzip.open(mycsv,'w')
            for row in reader:
               f.write(row[2]+";"+row[3]+";"+row[5]+"\n")
            f.close()
         if debug:
            print "[+] Custom check SWAP eseguito"
      except IndexError:
         print "[-] Il sistema ha subito un riavvio, dati SWAP parzialmente corrotti"
      return 1

   if export != "S" and export != "u" and export != "r" and export != "q":
      print "[-] Argomento export errato!"


def store_csv(debug):
   ram = csv_custom_path['ram']
   ldavg = csv_custom_path['ldavg']
   cpu = csv_custom_path['cpu']
   swap = csv_custom_path['swap']
   for csvfile in [ram, ldavg, cpu, swap]:
      shutil.move(csvfile, ConfigSectionMap("export")['directory'])
      if debug:
          print "[+] Copiato csvfile in %s" % ConfigSectionMap("export")['directory']

def clean_tmp(debug):
   try:
      shutil.rmtree(tmppath)
      if debug:
         print "[+] rimossa la directory temporanea:" + tmppath
   except OSError, e:
      if e.errno != 2:
         raise
   return 1

def main():
   debug = int(ConfigSectionMap("debug")['debug'])
   set_tmp_path(debug)
   check_data_path(debug)
   global csv_custom_path
   csv_custom_path = {}
   for export in ["r", "u", "q", "S"]:
      custom_csv(set_monthday(yesterday(), debug), export, debug)
   store_csv(debug)
   clean_tmp(debug)

if __name__ == "__main__":
   main()
