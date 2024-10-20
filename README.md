# SIESTA Incremental Constraints Mining Module

This project implements an intermediate extension of SIESTA: a Scalable Infrastructure for Sequential Pattern Analysis. It utilizes the indices built by the Preprocess Component to efficiently respond to Declare constraints queries. It serves as an alternative to SIESTA's Query Executor (QP) for constraints mining, enabling incremental extraction with better performance in terms of time.

The main difference from the QP method is that Incremental Mining preserves intermediate constraints states after a new log batch file is indexed. Therefore, multiple queries to this module, when no new data has been added, require constant time to get a response.
