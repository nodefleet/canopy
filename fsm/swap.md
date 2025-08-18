# swap.go - Token Swapping in the Canopy Blockchain

This file contains the state machine changes related to token swapping functionality in the Canopy blockchain. It implements a cross-chain token exchange mechanism that allows users to trade tokens between different chains in a secure, deterministic way.

## Overview

The token swapping system enables:
- Creating sell orders for tokens on the Root Chain
- Locking orders when a buyer commits to the exchange
- Completing swaps when payment is confirmed
- Resetting orders if deadlines are missed
- Monitoring cross-chain transactions to validate swap completion

## Core Components

### Order Book Management

The order book is a central component that tracks all sell orders for a specific chain. It maintains information about:
- Available tokens for sale
- Requested amounts for exchange
- Seller and buyer addresses
- Deadlines for transaction completion

The system provides functions to create, edit, lock, reset, and close orders within the order book. Each order has a unique ID and contains details about the assets being exchanged, the parties involved, and the current status of the swap.

### Cross-Chain Transaction Monitoring

The system monitors transactions across different chains to verify when buyers have sent the requested tokens to sellers. This involves:
- Parsing transaction memos for embedded commands
- Verifying payment amounts match the order requirements
- Checking that payments are sent to the correct addresses
- Confirming transactions occur before deadlines

When a valid payment is detected, the system can close the order and release the escrowed tokens to the buyer.

### Escrow Management

To ensure secure exchanges, the Root Chain holds tokens in escrow until the swap is completed:
- When a sell order is created, tokens are moved to an escrow pool
- When a swap is completed, tokens are released from escrow to the buyer
- If an order is deleted or reset, tokens can be returned to the seller

This escrow mechanism provides security for both parties in the exchange.

### Token Swap Workflow

The token swap process follows these steps:

1. **Order Creation**: A seller creates a sell order, specifying the amount of Root Chain tokens for sale and the amount of buyer-chain tokens requested in return. The Root Chain tokens are placed in escrow.

2. **Order Locking**: A buyer locks an order by providing their receive address and setting a deadline by which they must complete the payment. This reserves the order for that specific buyer.

3. **Payment**: The buyer sends the requested tokens directly to the seller's address on the buyer chain, including a CloseOrder message in the transaction memo.

4. **Verification**: The Committee (validators) witnesses the payment transaction on the buyer chain and verifies that the correct amount was sent to the correct address before the deadline.

5. **Order Closing**: If verification is successful, the Committee closes the order and releases the escrowed Root Chain tokens to the buyer's address.

6. **Order Reset**: If the buyer fails to send payment before the deadline, the order is reset and becomes available for other buyers.

This workflow ensures that tokens are exchanged securely without requiring trust between the parties.

### Deadline Management

The system uses blockchain heights as deadlines rather than timestamps:
- When an order is locked, a deadline is set based on the current height plus a configurable number of blocks
- The system checks if orders have expired by comparing the current height to the deadline
- Expired orders are automatically reset to be available again

Using block heights provides a deterministic way to measure time that all validators can agree on.

### Order Creation and Management

When a user wants to sell tokens:
1. They create a sell order specifying the amount to sell and the amount they want in return
2. The tokens are moved to an escrow pool associated with the specific chain ID
3. The order is added to the order book and becomes visible to potential buyers
4. The seller can edit or delete the order as long as it hasn't been locked by a buyer

### Committee Validation

The Committee (validators) plays a crucial role in the token swap process:
1. It witnesses lock orders on the buyer chain and updates the Root Chain order book
2. It monitors for payment transactions on the buyer chain
3. It verifies that payments match the order requirements
4. It processes close orders when payments are confirmed
5. It resets orders when deadlines are missed

This validation ensures that the swap process is secure and that both parties fulfill their obligations.

### Cross-Chain Communication

The token swap system enables communication between different chains:
1. The Root Chain maintains the order book and escrows the seller's tokens
2. The buyer chain is where the payment transaction occurs
3. The Committee observes both chains and updates the Root Chain state based on buyer chain events
4. Special messages embedded in transaction memos (LockOrder, CloseOrder) facilitate cross-chain communication

This cross-chain communication allows for secure token exchanges without requiring direct integration between the chains.

## Security & Integrity Mechansisms

- **Escrow**: Seller tokens are held in escrow until the swap is completed
- **Deadlines**: Time-limited transactions prevent indefinite locks on orders
- **Verification**: Multiple validators verify payment transactions
- **Deterministic Parsing**: Transaction parsing follows strict rules to ensure all validators reach the same conclusion
- **Historical Checking**: The system can check previous blocks to confirm order status
