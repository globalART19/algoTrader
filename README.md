Welcome to the Trahan Automated Trading platform

This is not even close to complete yet, so don't try to use it. Getting closer now!

Best,
Andrew

In progress:
- Create algorithm that functions for new/incoming data.
- Add buy/sell trigger information to output

To Do:
- Determine best way to reference new/current data pulls.
  * Have incoming data draws push to the history db collection?
- Start building authorized client trading structure
- popHist store missing data points to update later
- Fix CTRL-C not quitting all loops
- Add proper threading threading.Event() calls
- Make threads into a class
- Fix print screen overlay to keep ">>>" at bottom
- Build basic UI (HTML/CSS)
- Create proper indexes in Mongo to increase efficiency
- Linear or polynomial regression across l2current data for analysis?
- Remove ticker data draw to mongo? Shouldn't be needed?

Latest:
- Repair "updateHist" function

Completed:
- Build authorized client trading testing structure
- Finish building algorithm
- Updated graph to show buy/sell points
- Start building algorithm for sandbox testing
- Build sandbox client for algorithm testing
- Calculation graph x-axis to NOT seconds
- Add proper notes to code for clarity
- Repaired RSI indicator.
- Fixed minor calcPopulateBulk bug3
- Add calculation graph
- Add RSI calculation.
- calculations are iterating backwards. Flip iteration and reduce data draw.
- Improve efficiency for calcPopulate
- calcPopulate wayyy to slow. Increase Efficiency
- Added basic incomplete data/spotty server handling to popHistory
    (only missed 830/158400 on latest run!!)
- Calcs broken. Fix it!
- Add "updateHist" function
- Clean up L2 notes
- Create proper indexes in Mongo (done for level2current)
- Improve efficiency for lUpdateData
- Add MongoDB launch on startup
- Fix calcPopulate dump to array
- Fix calcPopulation to $push properly
- Start working on DataFunc and building that out!
- Update HistData to loop over selected time range
- Add initial error handling to l2Graph()
- Level 2 works!
