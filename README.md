# elephant56

elephant56 is a Genetic Algorithms (GAs) framework for Hadoop MapReduce with the aim of easing the development of distributed GAs. It provides high level functionalities which can be reused by developers, who no longer need to worry about complex internal structures.

## Features

- Sequential Genetic Algorithm
- Parallel Genetic Algorithm
  - Global model, also called master-slave model
  - Grid model, also called cellular model or fine-grained parallel model
  - Island model, also called distributed model or coarse-grained parallel model
- Report of execution time and population evolution
- Sample individual and genetic operator implementations
  - Number sequence individuals, roulette wheel selection, single point crossover, etc.

## Usage

1. Compile the library with Maven
2. Import the library in a new project
3. Extend `it.unisa.elephant56.user` classes (optional)
4. Choose a driver (sequential or parallel)
6. Register the individual and genetic operator classes to the driver
5. Insert `driver.run()`
6. Pack everything in a JAR
7. Run with Hadoop!

## License

elephant56 is licensed under the terms of the [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html). Please see the [LICENSE](LICENSE.md) file for full details.

## Credits

[Pasquale Salza](mailto:psalza@unisa.it) - Department of Computer Science, University of Salerno, Italy

[Filomena Ferrucci](mailto:fferrucci@unisa.it) - Department of Computer Science, University of Salerno, Italy

[Federica Sarro](mailto:f.sarro@ucl.ac.uk) - Department of Computer Science, University College London, United Kingdom
