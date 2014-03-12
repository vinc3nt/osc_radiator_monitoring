#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import csv
import tempfile
import datetime
import sys
import gzip
from collections import defaultdict
import shutil
import os
import ConfigParser

# Config file
Config = ConfigParser.ConfigParser()
Config.read('cron.cfg')

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

def set_tmp_path(debug):
   """Crea una directory temporanea"""
   global tmppath
   tmppath = tempfile.mkdtemp()
   if debug:
      print "[+] Creata directory: " + tmppath
   return tmppath

def check_data_path(debug):
   """ Verifica l'esistenza della directory settata nella
   configurazione e se non esiste la crea """
   directory = ConfigSectionMap("export")['directory']
   if not os.path.exists(directory):
       os.makedirs(direcotory)
       if debug:
           print "[+] Creata directory"


def yesterday():
   """Ritorna il giorno di ieri"""
   today = datetime.date.today()
   yesterday = today - datetime.timedelta(1)
   return yesterday

def today():
   """Ritorna il giorno di oggi in formato YYYYMMDD"""
   today = datetime.date.today()
   return today.strftime("%Y%m%d")

def yday():
   """ Ritorna giorno, mese ed anno di oggi formattati per la funzione
   count_auth """
   yday = datetime.datetime.today() - datetime.timedelta(1)
   return yday.year, yday.month, yday.day, 0, 0

def tday():
   """ Ritorna giorno, mese ed anno di ieri formattati per la funzione
   count_auth """
   tday = datetime.datetime.today()
   return tday.year, tday.month, tday.day, 0, 0

def logfilename():
   """ Ritorna il path ed il filename del file di log di accounting
   da analizzare """
   return '%s-%s' % (ConfigSectionMap("log")['auth'], today())

def zero_auth(start, end, delta):
   """ Ciclo per produrre tutti i secondi della giornata """
   current = start
   while current < end:
      yield current
      current += delta

def count_auth(log, debug):
   """ Crea un dizionario che ha come chiave il
   timestamp e come valore il numero delle autenticazioni
   andate a buon fine, mentre imposta a 0 tutti i secondi
   senza alcun riferimento nel file di log """
   global authentications
   authentications = defaultdict(int)
   for result in zero_auth(datetime.datetime(*yday()), datetime.datetime(*tday()), datetime.timedelta(seconds=1)):
      x = result.strftime("%Y-%m-%d %H:%M:%S")
      authentications[x] = 0
   for line in log:
      if line.strip().endswith('ACCEPT'):
         time = line[:19]
         authentications[time] += 1
         print authentications[time] #### only for debug issues
   if debug:
      print "[+] Creato il dictionary delle authentications"
   return authentications

def auth_csv(dictionary, debug):
   """Converte il dizionario in un csv gzippato"""
   csvpath = '%s/auth-%s_%s.csv.gz' % (tmppath, yesterday(), ConfigSectionMap("client")['hostname'])
   f = gzip.open(csvpath,'wb')
   for key in sorted(dictionary.keys()):
      f.write(str(key) + ";" + str(dictionary[key]) + "\n");
   if debug:
      print "[+] Dati autenticazione importati nel csv"
   f.close()
   return csvpath

def store_csv(csvpath, debug):
   """ Copia solo i csv dalla dir temporanea alla
   dir settata nella configurazione """
   shutil.move(csvpath, ConfigSectionMap("export")['directory'])
   if debug:
       print "[+] %s copiato nella dir: %s" % (csvpath, ConfigSectionMap("export")['directory'])

def clean_tmp(debug):
   try:
      shutil.rmtree(tmppath)
      if debug:
         print "[+] rimossa la directory temporanea:" + tmppath
   except OSError, e:
      if e.errno != 2:
         raise

def main():
   debug = int(ConfigSectionMap("debug")['debug'])
   set_tmp_path(debug)
   try:
      log = open(logfilename(), 'r')
      count_auth(log, debug)
      auth_csv(authentications, debug)
      csvpath = '%s/auth-%s_%s.csv.gz' % (tmppath, today(), ConfigSectionMap("client")['hostname'])
      log.closed #chiudo il file di log di autenticazione originale
   except IOError:
      if debug:
         print "[-] Il file " + logfilename() + " non e' presente"
      return 1
   store_csv(csvpath, debug)
   clean_tmp(debug)
   return 0

if __name__ == '__main__':
   main()
