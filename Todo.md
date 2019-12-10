n-cube 'ToDo' list
======
### n-cube engine
* Bring difficult computer science problems to non-programmers / business people:
 * AI / Optimizations / Solvers (Artificial Intelligence, Machine Learning, Predictive)
  * Add new train() and predict() APIs to make it dirt-simple for business folks to have access to a neural net
  * Genetic algorithm support for optimization and other such problems
  * Complete Constraint Satisfaction Problem (CSP) support
  * Natural language processing support
 * Infinite sized axis / n-cubes (process n-cube in a stream-like way where the entire n-cube is not in memory)
 * Lucene-like index capability for axis matching / indexing, as well as cube content indexing
 * Graph processing routines
 * Game theory
 * Decision theory
 * Simulated annealing
 * Swarm theory
 * Run all tests

### n-cube editor (NCE)
* Run all tests


=== Ideas to improve NCE Testing ===
Make the "value" side of an input a bigger text box window.. or just create make an entire text box area for defining all inputs.
All tests on an NCube would be run on "commit" (as opposed to at each change).
Each test can be enabled or disabled (including "enable all" and "disable all" buttons).  Disabled tests don't get run, but still exist.  It would be useful to have tests for an NCube that don't run during builds, but which you can execute manually - For example, tests you've created for when refactoring an NCube).
Track coverage for "cells hit" by tests (indicate with a border color or a couple of pixels in a corner).
Track coverage for cells with groovy expression contents.
 
Improve generation of tests
                Create a dialog/modal for generating tests (should be able to use this regardless of whether there are existing tests or not)
                                Option to create a test for every combination of Axis/Columns
                                                Option to populate the output.return results with the actual values
                                Option to create a test for a selected set of combinations of Axis/Columns
                                                Option to populate the output.return assertions with the actual values
                               
                Batch creating of inputs
 
 
Improve test navigation (right-hand nav window)
                Add a scrollbar  - Currently you cannot see all tests if there are more than fit on the screen.
                Add a little colored dot to the left of the test name to indicate if the test passes (green), fails (red), is disabled (blue), or has not been run yet (grey).   
                Add ability to control-click and shift-click to multiselect tests (for use with the delete and duplicate actions as well as to be able to execute multiple tests at once).
                Add ability to auto-sort or manual sort tests.  Or maybe just autosort them rather than doing it by ceate date or whatever it does now.
 
Ability to copy tests from one NCube to another.
 
=== Non-Testing related NCE enhancements/fixes ===
Double-clicking on the groovy section of a Rule column should open a Groovy expression editor.  Double-clicking on the rule name should open the Metaproperties.
Adding a new rule column should give a value of "false", not "newValue" just so it's not triggered until you configure it...for dummies like me that accidentally add one two many rules and didn't realize it and then can't figure out why the ncube doesn't work. ;)