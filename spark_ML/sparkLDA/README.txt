Put the functions into a structured way for the ML model training. In order to
import the functions for the ML training using Spark, one needs to:
1. Zip the module into a zip file, and the folder should be top-level directory
where the __int__.py file can be found. In other words, creating a source
distribution with the following structure will not work:
sparkLDAPackage
└── sparkLDA
    └── __init__.py
2. In this case, zip the sparkLDA folder. Here the setup.py file
does not matter since we don't actually create a python distribution here.
3. __init__.py is required to import the zip directory as a package,
and can simply be an empty file.
4. You need to add the following option when submit your spark job to the cluster:
"--py-files sparkLDA.zip" if your sparkLDA.zip file doesn't reside on the hdfs system.
By doing this, all worker nodes can find the zip file.
5. You need to configure your spark session with
"sc.addPyFile("sparkLDA.zip")"
so that the directory is added to the path and the functions can be properly loaded.