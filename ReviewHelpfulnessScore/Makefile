OBJS=pa2/*.class
MAIN_FUNC_PACKAGE=com.sun.tools.javac.Main
EXE=pa2/PA2.jar
JAR_ARGS=cf

all: pa2/driver.class
	jar $(JAR_ARGS) $(EXE) $(OBJS)

pa2/driver.class: pa2/Driver.java
	$(HADOOP_HOME)/bin/hadoop $(MAIN_FUNC_PACKAGE) pa2/Driver.java

run: $(EXE)
	$(HADOOP_HOME)/bin/hadoop jar $(EXE) pa2/Driver $(INPUT)

clean:
	rm -rf pa2/*.class $(EXE) *output/ pa2/*output pa2/*intermediate *intermediate *idf *term_freq *tfidf *summary
