## gaibo_modules - miscellaneous Python utilities

**If you have ever been forced by Gaibo to add *P:/PrdDevSharedDB/Gaibo's Modules/* into your IDE's PYTHONPATH in order to use his functions, please consider removing that and cloning this repository instead.**

---

The modules:

* *bonds_analytics* - Treasury notes/bonds-related calculations; e.g. yield to maturity, duration, futures conversion factor, futures delivery basket

* *cboe_exchange_holidays_v3* - trading calendar tools

* *cme_eod_file_reader* - a whole module dedicated to the complexities of reading CME's EOD Treasury options data files; *read_cme_file* is the function you want

* *hanweck_eod_file_reader* - a whole module dedicated to the complexities of reading Hanweck's EOD Treasury futures and options data files; *read_hanweck_file* is the function you want

* *options_analytics* - Black-76 Greeks

* *options_data_tools* - common operations performed on DataFrames of options data; e.g. add a risk-free rates column, calculate a forward price column

* *options_futures_expirations_v3* - options and futures expiry-related date tools

* *timer_tools* - tools for timing code

* *treasury_futures_reader* - a whole module dedicated to pulling Bloomberg Treasury futures data and reading it; NOTE: has dependency on pdblp (pandas-friendly Bloomberg API package)

* *treasury_rates_reader* - *get_rate* is the function that will get you any Treasury yield curve-based rate you can think of; NOTE: has dependency on Anshul's *metaballon* package, so please get access to that either through cloning his repository or adding his package into PYTHONPATH

* *web_tools* - tools for pulling data from web APIs

* *xtp_eod_file_reader* - a whole module dedicated to the complexities of reading Jerry's XTP-captured EOD Treasury options prices snapshots (.txt format); *read_xtp_file* is the function you want
