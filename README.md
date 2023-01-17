# MBD-Reddit
network analysis of reddit comments

How to get GraphFrames to work:

mvn org.apache.maven.plugins:maven-dependency-plugin:2.1:get -Dartifact=graphframes:graphframes:0.8.0-spark2.4-s_2.11 -DrepoUrl=https://repos.spark-packages.org/
pyspark --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11
spark-submit --packages graphframes:graphframes:0.8.0-spark2.4-s_2.11 file_name.py

Documentation:
https://graphframes.github.io/graphframes/docs/_site/index.html
https://graphframes.github.io/graphframes/docs/_site/user-guide.html
https://graphframes.github.io/graphframes/docs/_site/api/python/index.html
