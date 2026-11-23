Configuration Management
========================

The DTPatternRecognition framework uses a centralized configuration system to manage all analysis settings,
ensuring consistency and flexibility throughout your workflow. This is handled by a global configuration
class, ``Config``, which loads YAML files and exposes their contents as Python attributes.

The configuration is managed by a single global instance, ``RUN_CONFIG``, imported from ``dtpr.base.config``.
This instance is initialized at startup with a specific YAML configuration file (typically ``run_config.yaml``),
and all top-level keys in this file become attributes of ``RUN_CONFIG``. This design allows you to access and
modify configuration parameters from anywhere in your codebase without hardcoding values.

The configuration file controls:
- Particle and event initialization
- Preprocessing and selection logic
- Histogram definitions
- Plotting styles and options
- Any other analysis tool settings

.. rubric:: Example: Accessing Configuration Settings

A snippet from ``run_config.yaml`` might look like:

.. code-block:: yaml

    particle_types:
      digis:
        amount: 'digi_nDigis'
        attributes:
          wh:
            branch: 'digi_wheel'
          BX:
            expr: 'time // 25 if time is not None else None'
        sorter:
          by: 'p.BX'

    ntuple_preprocessors:
      genmuon_matcher:
        src: "dtpr.utils.genmuon_functions.analyze_genmuon_matches"

    histo_names:
      - seg_eff_MB1
      - AM_rate_allBX_MB1
      - LeadingMuon_pt

    plot_configs:
      mplhep-style: 'CMS'
      figure-configs:
        figure.dpi: 100

You can access these settings directly in your Python code:

.. code-block:: python

    from dtpr.base.config import RUN_CONFIG

    # Accessing particle type definitions
    digi_config = RUN_CONFIG.particle_types["digis"]
    print(f"Digi particle amount source: {digi_config['amount']}")
    print(f"Digi BX attribute expression: {digi_config['attributes']['BX']['expr']}")

    # Accessing preprocessor definition
    genmuon_matcher_src = RUN_CONFIG.ntuple_preprocessors["genmuon_matcher"]["src"]
    print(f"Genmuon matcher function source: {genmuon_matcher_src}")

    # Accessing list of histogram names to fill
    print(f"First histogram to fill: {RUN_CONFIG.histo_names[0]}")

    # Accessing plotting style
    print(f"Matplotlib HEP style: {RUN_CONFIG.plot_configs['mplhep-style']}")

.. rubric:: Output

.. code-block:: text

    Digi particle amount source: digi_nDigis
    Digi BX attribute expression: time // 25 if time is not None else None
    Genmuon matcher function source: dtpr.utils.genmuon_functions.analyze_genmuon_matches
    First histogram to fill: seg_eff_MB1
    Matplotlib HEP style: CMS

This demonstrates how ``RUN_CONFIG`` centralizes all settings, making them easily accessible throughout 
the application without hardcoding values or logic into Python files.

.. rubric:: Changing the Configuration File

A key feature of ``RUN_CONFIG`` is its ability to switch between different configuration files at runtime. 
This is useful for running multiple analysis scenarios, testing different event setups, or experimenting with parameter variations.

You can change the configuration file using the ``change_config_file`` method:

.. code-block:: python

    from dtpr.base.config import RUN_CONFIG
    import os

    # Print initial config file and style
    print(f"Initial config file: {RUN_CONFIG.path}")
    print(f"Initial Matplotlib HEP style: {RUN_CONFIG.plot_configs['mplhep-style']}")

    # Load a custom configuration file
    custom_config_path = os.path.abspath("my_custom_config.yaml")
    RUN_CONFIG.change_config_file(config_path=custom_config_path)
    print(f"\nNew config file: {RUN_CONFIG.path}")
    print(f"New Matplotlib HEP style: {RUN_CONFIG.plot_configs['mplhep-style']}")

    # Switch back to the default config
    RUN_CONFIG.change_config_file(config_path=os.path.abspath("dtpr/utils/yamls/run_config.yaml"))
    print(f"\nSwitched back to default. Matplotlib HEP style: {RUN_CONFIG.plot_configs['mplhep-style']}")

**Output:**

.. code-block:: text

    Initial config file: .../dtpr/utils/yamls/run_config.yaml
    Initial Matplotlib HEP style: CMS

    New config file: .../my_custom_config.yaml
    New Matplotlib HEP style: ATLAS

    Switched back to default. Matplotlib HEP style: CMS


.. rubric:: CLI Configuration

There is also a separate configuration instance, ``CLI_CONFIG``, intended for internal use by the 
command-line interface tools. Regular users should interact only with ``RUN_CONFIG`` for analysis 
configuration. ``CLI_CONFIG`` is primarily for developers and advanced CLI customization.

.. automodule:: dtpr.base.config
    :members:
    :undoc-members:
    :exclude-members: __weakref__