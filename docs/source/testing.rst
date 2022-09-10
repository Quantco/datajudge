Testing
=======

While ``datajudge`` allows to express expectations via specifiations, ``Requirement`` s and ``Constraint`` s, the execution of tests is delegated to ``pytest``. As a consequence, one may use any functionalities that ``pytest`` has to offer. Here, we want to illustrate some of these functionalities that might turn out useful.

TODO: Mention that here we don't use the helper function and how that usually works.

Subselection
------------

subtitle
********

TODO: Create subheaders for both methods and express how they are different (ex-post vs ex-ante).

In some scenarios one might want to execute only some tests derived from a specification. This could, for instance be the case when still in an early phase of constructing a specification or after having fixing the data as a consequence of a failing test.

Thankfully, this can be done particularly conveniently.

Instead of running ``pytest specification.py`` one may simply use pytests's ``-k`` flag and specify the ``Constraint`` (s) one cares about. E.g. If only caring about ... this could look as follows:

.. code-block:: console

  $ pytest twitch_specification.py -k "UniquesEquality::public.twitch_v1"

Another option to subselect a certain set of tests is by use of `pytest markers <TODO: INSERT URIL>_`. The following is one way of going about this.

First, we'll add a bit of pytest magic to the respective ``conftest.py``:

.. code-block:: python

  def pytest_generate_tests(metafunc):
      if "basic_constraint" in metafunc.fixturenames:
           metafunc.parametrize(
	       "basic_constraint",
               metafunc.module.get_basic_constraints(),
               ids=metafunc.module.idfn,
	   )
      if "constraint" in metafunc.fixturenames:
          metafunc.parametrize(
	      "constraint",
	      metafunc.module.get_all_constraints(),
	      ids=metafunc.module.idfn,
          )


.. code-block::

   [pytest]
   addopts = --strict-markers
   markers = basic: basic specification
	     all: entire specification


In case you don't know about ``conftest.py`` files, you can read up on them `here <https://TODO>`_. The gist of it is that they configure how pytest actually runs tests for all files in the current directory. Once that is taken care of, you can adapt your specification as follows:

.. code-block:: python

  def get_basic_requirements() -> List[Requirement]:
      ...

  def get_advanced_requirements() -> List[Requirement]:
      ...

  def get_basic_constraints() -> List[Constraint]:
      return [constraint for requirement in get_basic_requirements() for constraint in requirement]

  def get_all_constraints() -> List[Constraint]:
      all_requirements = get_basic_requirements() + get_advanced_requirements()
      return [constraint for requirement in all_requirements for constraint in requirement]

  @pytest.mark.basic
  def test_basic_constraint(basic_constraint: Constraint, datajudge_engine):
      test_result = basic_constraint.test(datajudge_engine)
      assert test_result.outcome, test_result.failure_message

  @pytest.mark.all
  def test_all_constraint(constraint: Constraint, datajudge_engine):
      test_result = constraint.test(datajudge_engine)
      assert test_result.outcome, test_result.failure_message

Once these changes are taken care of, you may run

.. code-block:: console

  $ pytest specification.py -m basic

to only test the basic ``Requirement`` s or

.. code-block:: console

  $ pytest specification.py -m all

to test all ``Requirement`` s.


Parametrization
---------------

If you

.. code-block:: python
  :caption: ``conftest.py``

  def pytest_addoption(parser):
      parser.addoption("--new_db", action="store", help="name of the new database")
      parser.addoption("--old_db", action="store", help="name of the old database")


  def pytest_generate_tests(metafunc):
      params = {
          "db_name_new": metafunc.config.option.new_db,
          "db_ne_old": metafunc.config.option.old_db,
      }
      metafunc.parametrize(
          "constraint",
          metafunc.module.get_constraints(params),
          ids=metafunc.module.idfn,
      )

and

.. code-block:: python
  :caption: ``specification.py``

  def get_requirements(params):
      ...
      return requirements


  def get_constraints(params):
      return [
	  constraint for requirement in get_requirements(params) for constraint in requirement
      ]


  def idfn(constraint):
      return constraint.get_description()


  def test_constraint(constraint, datajudge_engine):
      test_result = constraint.test(datajudge_engine)
      assert test_result.outcome, test_result.failure_message


then you can

.. code-block:: console

  $ pytest specification.py --new_db=db_v1 --old_db=db_v2

Html reports
------------

By default, running ``pytest`` tests will output test results to one's respective shell.
Alternatively, one might want to generate an html report summarizing and expanding on
all test results. This can be advantageous for

* Sharing test results with colleagues
* Archiving and tracking test results over time
* Make underlying sql queries conveniently accessible

Concretely, such an html report can be generated by
`pytest-html <https://github.com/pytest-dev/pytest-html>`_. Once installed, using it as simple
as appending ``--html=myreport.html` to the ``pytest`` call.

In our twitch example, this generates `this html report <https://github.com/Quantco/datajudge/tree/main/docs/source/examples/twitch_report.html>`_.


Retrieving queries
------------------

We not only care about knowing whether there is a problem with the data at hand. Rather,
we would also like to assist in solving it as fast as possible. For that matter datajudge
makes the queries it uses to assert predicates for testing available via the ``TestResult``
class. That way, if a test is failing, the user can jumpstart the investigation of the
problem by reusing and potentially adapting the underlying queries.

Instead of simply running ``assert constraint.test(engine).outcome``, one may add
the ``TestResult`` 's ``logging_message`` to e.g. a ``logger`` or add it to pytest
``extra``:

.. code-block:: python

  from pytest_html import extras

  def test_constraint(constraint: Constraint, engine, extra):
    test_result = constraint.test(engine)
    message = test_result.logging_message

    if not test_result.outcome:
      # Send to logger.
      logger.info(message)
      # Add to html report.
      extra.append(
        extras.extra(
          content=message,
          format_type="text",
          name="failing_query",
          mime_type="text/plain",
          extension="sql",
        )
      )

   assert test_result.outcome


Such a ``logging_message`` can look as follows, with ready to execute sql queries:

.. code-block:: sql

  /*
  Failure message:
  tempdb.public.twitch_v1's column(s) 'language' doesn't have the
  element(s) '{'Sw3d1zh'}' when compared with the reference values.
  */

   --Factual queries:
   SELECT anon_1.language, count(*) AS count_1
  FROM (SELECT public.twitch_v1.language AS language
  FROM public.twitch_v1) AS anon_1 GROUP BY anon_1.language

  -- Target queries:
   SELECT anon_1.language, count(*) AS count_1
  FROM (SELECT public.twitch_v2.language AS language
  FROM public.twitch_v2) AS anon_1 GROUP BY anon_1.language


If using a mechanism - as previously outlined - to forward these messages to
an html report, this can look as follows:


.. image:: report_failing_query1.png
  :width: 800


.. image:: report_failing_query2.png
  :width: 800

