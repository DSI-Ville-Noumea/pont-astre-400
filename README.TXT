ATTENTION, la passerelle s'exécute sur l'AS400, donc JAVA 1.5

Pour installer le driver Oracle dans le repo local :
mvn install:install-file -Dfile=./lib/classes12.jar -DgroupId=com.oracle -DartifactId=classes12 -Dversion=10.2.0.3 -Dpackaging=jar

#Pour installer JT400 dans le repo local :
mvn install:install-file -Dfile=./lib/jt400-8.2.jar -DgroupId=net.sf.jt400 -DartifactId=jt400 -Dversion=8.2 -Dpackaging=jar