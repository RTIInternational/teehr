.. "TEEHR: Tools for Exploratory Evaluation in Hydrologic Research" documentation master file, created by
   sphinx-quickstart on Mon Jan 29 10:49:57 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image: ../images/teehr.png

********************
TEEHR documentation
********************

TEEHR (pronounced "tier") is a python tool set for loading, storing, processing and visualizing hydrologic data, particularly National Water Model data, for the purpose of exploring and evaluating the datasets to assess their skill and performance.

.. grid:: 1 2 2 2
    :gutter: 4
    :padding: 2 2 0 0
    :class-container: sd-text-center

    .. grid-item-card:: Getting started
        :img-top: _static/index_getting_started.svg
        :class-card: intro-card
        :shadow: md

        New to *TEEHR*? Check out the getting started guides. They contain an
        introduction to *TEEHR'* main concepts and links to additional tutorials.

        +++

        .. button-ref:: getting_started
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            To the getting started guides

    .. grid-item-card::  User guide
        :img-top: _static/index_user_guide.svg
        :class-card: intro-card
        :shadow: md

        The user guide provides in-depth information on the
        key concepts of pandas with useful background information and explanation.

        +++

        .. button-ref:: user_guide
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            To the user guide

    .. grid-item-card::  API reference
        :img-top: _static/index_api.svg
        :class-card: intro-card
        :shadow: md

        The reference guide contains a detailed description of
        the TEEHR API. The reference describes how the methods work and which parameters can
        be used. It assumes that you have an understanding of the key concepts.

        +++

        .. button-ref:: autoapi
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            To the reference guide

    .. grid-item-card::  Developer guide
        :img-top: _static/index_contribute.svg
        :class-card: intro-card
        :shadow: md

        Saw a typo in the documentation? Want to improve
        existing functionalities? The contributing guidelines will guide
        you through the process of improving TEEHR.

        +++

        .. button-ref:: development
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            To the development guide


.. grid:: 1 2 3 4

    .. grid-item-card::
        :img-top: https://ciroh.ua.edu/wp-content/uploads/2022/08/CIROHLogo_200x200.png
        :columns: 3
        :class-card: ciroh-card
        :link: https://ciroh.ua.edu/


    .. grid-item-card::
        :columns: 9
        :class-card: ciroh-card
        :margin: 4 0 0 0

        Funding for this project was provided by the National Oceanic
        & Atmospheric Administration (NOAA), awarded to the Cooperative
        Institute for Research to Operations in Hydrology (CIROH) through
        the NOAA Cooperative Agreement with The University of Alabama (NA22NWS4320003).


.. toctree::
   :hidden:
   :titlesonly:
   :maxdepth: 3

   getting_started/index
   user_guide/index
   development/index
   changelog/index
