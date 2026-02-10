Initial build questions and notes:

Clarifying questions to make implementation seamless and responsive
1) Product behavior / UX contract
Should the app treat “obs vs forecast” as two separate tabs/workflows, or a single unified workflow with dynamic controls?
** obs vs forecast should be a single unified workflow

For the point click, should the app always use nearest grid cell, or allow interpolation (bilinear)?
** nearest gridcell is fine

When users click outside raster coverage, what should happen (disable plots, snap to edge, warning)?
** warning to move map, and click "refresh raster" button to reload

Do you want a single selected point only, or allow comparing multiple clicked points?
** single point

Should pre-canned points be purely visual markers, or also selectable shortcuts that trigger data load?
** selectable shortcuts that put names on the plots

2) Dataset catalog + source selection
Which exact forecast sources must be in v1 (GEFS, GFS, HRRR, ECMWF ENS, others)?
** put the GEFS and ECMWF ENS in v1

Should available sources/variables be hardcoded config or dynamically discovered from metadata/catalog at runtime?
** hardcoded with a set of instructions in a readme file for how to update later

Are authenticated URLs required for production (email/query token, API key, signed URL), and how should secrets be managed?
** no, they are not required

3) Variable harmonization
Do you want a canonical variable dictionary (e.g., “2m Temperature” => source-specific variable IDs)?
** yes, I want a canonical variable dictionary

How should unit differences be handled (e.g., Kelvin vs Celsius, precip units/rates vs accumulations)?
** Included in the dictionary should be a list of what units each variable is in by dataset. There should be an imperial vs metric selector and show all results aligned on the plot

If a variable exists in forecast but not obs (or vice versa), should the app hide that pairing or permit partial views?
** show partial views with an indicator of what data source the variable is missing from 

4) Time semantics (critical for correctness)
For forecast display, should time axis be valid_time only, or permit toggling to lead_time?
** allow toggling

For “forecast timeframe” selector, is the intent to choose by:

init run time,
valid date range,
lead-time window,
or a combination?
** the init runtime should be the most recent forecast, and the lead time window should default to 14 days in the future, but make this selectable

For raster default “tomorrow at noon,” should that be interpreted in UTC or user local timezone?
** User local time

How should multiple init runs be handled in spaghetti plots (latest only vs several recent runs)?
** use only latest run

5) Spaghetti + percentile definition
For ensemble sources, should spaghetti lines represent ensemble members at one init or multiple init runs (or both)?
** only one init run

For deterministic sources (no ensemble_member), what is expected for the spaghetti panel (single trace only, or pseudo-ensemble via recent runs)?
** single trace only

Percentiles (25/50/75): computed across ensemble members at each valid time, across runs, or both dimensions?
** using single init, percentiles should be across all values at each valid time (one set of values for each valid time representing the range of ensemble member values)

Should percentile lines include missing-member handling rules (minimum member count thresholds)?
** yes 

6) Raster map behavior
For “single forecast vs statistic across all forecasts,” which statistics are required (mean, median, min/max, spread/std, exceedance probability)?
** mean, median, min/max, 10/25/75/90 Percentiles

Should raster resolution dynamically downsample with zoom/extent to keep map interactions responsive?
** yes

Max acceptable raster request size/latency target (e.g., <1.5s for map pan/zoom updates)?
** start with <1.5s for updates

Should raster requests be throttled/debounced on map movement (e.g., load only on moveend)?
** yes

7) Geospatial overlays and boundaries
Do you require offline boundary layers bundled with app, or is live tile/vector service acceptable?
** live data is For

For overlays (countries, US states/counties, Canadian provinces), should users toggle each layer independently?
** no

Any preferred boundary sources/licenses (Natural Earth, TIGER/Line, Statistics Canada) for production use?
** no 

8) Performance architecture
Are you open to server-side caching (memoized xarray slices by source/variable/time/extent)?
** no since that is not supported on shinyapps.io

Preferred cache backend: in-memory only, disk cache, Redis, or cloud object cache?

Do you plan single-user personal hosting only, or multi-user concurrency (affects caching/session strategy)?
** single user

Do you want asynchronous loading with progress states/skeletons for map and chart panels?
** yes

9) Deployment and operations
Target deployment platform: Shiny Server, Posit Connect, Docker/Kubernetes, or shinyapps.io?
** shinyapps.io is where the final version will end up 

Any resource constraints (CPU/RAM) that should drive chunk sizes/downsampling strategy?

Should app log query metrics (latency, request sizes, cache hit rates) for tuning responsiveness?
** yes

10) Maintainability / update workflow
Do you want a source registry config file (YAML/JSON) so adding datasets requires no code changes?
** yes 

Should variable mappings be versioned and validated automatically (e.g., periodic schema check)?
** yes

How often should metadata refresh (startup only, scheduled, or user-triggered refresh)?
** user triggered
