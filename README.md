Welcome to the Trahan Automated Trading platform

This is not even close to complete yet, so don't try to use it.

In progress:
- Start building authorized client trading structure

To Do:
- Start building algorithm for sandbox testing
- popHist store missing data points to update later
- Do not calcPopulate until popHist data set is full and complete unless skip after # attempts
- Fix CTRL-C not quitting all loops
- Repair "updateHist" function
- Add proper threading threading.Event() calls
- Make threads into a class
- Fix print screen overlay to keep ">>>" at bottom
- Build basic UI (HTML/CSS)
- Create proper indexes in Mongo to increase efficiency
- Improve efficiency for calcPopulate
- Linear or polynomial regression across l2current data for analysis?
- Remove ticker data draw to mongo? Shouldn't be needed?

Latest:

Completed:
- calculations are iterating backwards. Flip iteration and reduce data draw.
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

Best,
Andrew
