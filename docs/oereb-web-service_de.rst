===================
oereb-web-service-Anleitung
===================

Überblick
=========

oereb-web-service ist ein in Java erstelltes Programm, das eine
"ÖREB-Webservice (Aufruf eines Auszugs)" implementiert.

oereb-web-service benötigt eine PostGIS Datenbank.

Laufzeitanforderungen
---------------------

Das Programm setzt Java 1.8 voraus.

Lizenz
------

GNU Lesser General Public License v2.1

Konfiguration
==============
Via application.properties im Verzeichnis in dem der Service gestartet wird. Oder entsprechende alternative
Konfigurationsmöglichkeiten von `Spring Boot <https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html>`_.

..code::
	
  spring.datasource.url=jdbc:postgresql:DBNAME
  spring.datasource.username=DBUSR
  spring.datasource.password=DBPWD
  spring.datasource.driver-class-name=org.postgresql.Driver
  oereb.dbschema=oereb
  oereb.cadastreAuthorityUrl=https://www.so.ch/verwaltung/bau-und-justizdepartement/amt-fuer-geoinformation
  oereb.planForLandregister=https://geo.so.ch/api/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng&TRANSPARENT=true&LAYERS=ch.so.agi.hintergrundkarte_farbig&STYLES=&SRS=EPSG%3A2056&CRS=EPSG%3A2056&TILED=false&DPI=96&OPACITIES=255&t=675&WIDTH=1920&HEIGHT=710&BBOX=2607051.2375,1228517.0374999999,2608067.2375,1228892.7458333333
  oereb.planForLandregisterMainPage=https://geo.so.ch/api/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng&TRANSPARENT=true&LAYERS=ch.so.agi.hintergrundkarte_farbig&STYLES=&SRS=EPSG%3A2056&CRS=EPSG%3A2056&TILED=false&DPI=96&OPACITIES=255&t=675&WIDTH=1920&HEIGHT=710&BBOX=2607051.2375,1228517.0374999999,2608067.2375,1228892.7458333333

