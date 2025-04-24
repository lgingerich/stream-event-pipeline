# Stream Event Pipeline

This project is split into two primary components: indexer and persistence

The indexer handles raw data indexing and persistence handles saving to storage, with
kafka acting as the message broker between these two components.

The home page for each of these components can be located here: 
- [indexer](./indexer/README.md)
- [persistence](./persistence/README.md)
