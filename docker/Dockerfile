FROM fokkodriesprong/docker-pyspark

ENV PATH $PATH:/usr/bin
RUN apt update && apt upgrade -y
RUN wget https://repo.continuum.io/miniconda/Miniconda3-4.2.11-Linux-x86_64.sh -O miniconda.sh
RUN bash miniconda.sh -bf -p /anaconda
ENV PATH /anaconda/bin:/usr/spark/bin:$PATH
RUN conda install ipython notebook jupyter pandas numpy scikit-learn -y
RUN pip install --upgrade pip
RUN pip install pytest coverage pytest-runner
ENV PYSPARK_PYTHON /anaconda/bin/python
ENV PYSPARK_DRIVER_PYTHON ipython
ENV PYSPARK_DRIVER_PYTHON_OPTS ""
EXPOSE 8888
