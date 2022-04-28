Motivation
==========

Ensuring data quality is important. ``datajudge`` seeks to make this convenient.

Not trying to reinvent the wheel, ``datajudge`` relies on ``pytest`` to test expectations against data. ``datajudge`` allows for the expression of expectations held against data stored in databases. In particular, it allows for comparing different ``DataSource`` s.


Comparisons between DataSources
-------------------------------

The data generating process can be obscure for a variety of reasons. In such scenarios one might ask the questions of

- Has the data 'changed' over time?
- Was the transformation of the data successful?

In both cases one might want to compare different data - either from different points in time or from different transformation steps - to each other.


Why not Great Expectations?
---------------------------

The major selling point is to be able to conveniently express expectations **between** different ``DataSource`` s. Great Expectations, in contrast, focuses on expectations against single ``DataSource`` s.

Moreover, some users have pointed out the following advantages:

- lots of 'query writing' is taken care of by having tailored ``Constraint`` s
- easier and faster onboarding
- assertion messages with counterexamples and other context information, speeding up the data debugging process
