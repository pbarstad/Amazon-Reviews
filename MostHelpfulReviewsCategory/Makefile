OBJS=*.class
MAIN_FUNC_PACKAGE=com.sun.tools.javac.Main
EXE=Driver.jar
JAR_ARGS=cf
OUTPUT_DIR=category_loader

all: driver.class
	jar $(JAR_ARGS) $(EXE) $(OBJS)

driver.class: Driver.java
	$(HADOOP_HOME)/bin/hadoop $(MAIN_FUNC_PACKAGE) Driver.java

run: $(EXE)
	$(HADOOP_HOME)/bin/hadoop jar $(EXE) Driver $(INPUT)

clean:
	rm -rf *.class $(EXE) $(OUTPUT_DIR)
