Motivation
==========

Ensuring data quality is of great importance for many use cases. ``datajudge`` seeks to make this convenient.

``datajudge`` allows for the expression of expectations held against data stored in databases. In particular, it allows for comparing different ``DataSource`` s. Yet, it also comes with functionalities to compare data from a single ``DataSource`` to fixed reference values derived from explicit domain knowledge.

Not trying to reinvent the wheel, ``datajudge`` relies on ``pytest`` to execute the data expectations.


Comparisons between DataSources
-------------------------------

The data generating process can be obscure for a variety of reasons. In such scenarios one might ask the questions of

- Has the data 'changed' over time?
- Was the transformation of the data successful?

In both cases one might want to compare different data -- either from different points in time or from different transformation steps -- to each other.


Why not Great Expectations?
---------------------------

The major selling point is to be able to conveniently express expectations **between** different ``DataSource`` s. Great Expectations, in contrast, focuses on expectations against a single ``DataSource``.

Moreover, some users have pointed out the following advantages:

- lots of 'query writing' is taken care of by having tailored ``Constraint`` s
- easier and faster onboarding
- assertion messages with counterexamples and other context information, speeding up the data debugging process
