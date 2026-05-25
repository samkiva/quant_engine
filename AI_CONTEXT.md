# Quant Engine Architecture

## Stack
- Python 3.13
- AsyncPG
- PostgreSQL
- Pandas
- NumPy
- SciPy

## Environment
- Termux Android
- Mobile constraints
- Limited RAM
- Slow IO/network

## Architecture Rules
- Async-first
- No blocking DB calls
- No lookahead bias
- Research modules isolated
- Statistical validation required

## Core Modules
- db/
- research/
- scripts/
- models/

## Current Objective
Validate volatility clustering hypothesis on BTCUSDT trades.

## Known Issues
- Batch fetch latency
- Memory pressure
- Query timeout risks

## Coding Standards
- Explicit typing
- Structured logging
- Pure feature functions
- Minimal coupling
