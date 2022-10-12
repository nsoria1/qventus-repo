FROM jupyter/minimal-notebook

RUN python --version

#RUN conda install --quiet --yes -c conda-forge osmnx dask

RUN pip install -U psycopg2-binary \
                   pandas \
                   numpy \
                   ipython-sql \
                   pandasql

VOLUME /notebooks

WORKDIR /notebooks