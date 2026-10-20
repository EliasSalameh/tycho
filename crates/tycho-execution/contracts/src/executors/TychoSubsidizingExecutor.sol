// SPDX-License-Identifier: LicenseRef-Fynd-License-1.1
pragma solidity ^0.8.26;

import {IExecutor} from "@interfaces/IExecutor.sol";
import {ISubsidyPayer, Subsidy} from "@interfaces/ISubsidyPayer.sol";
import {TransferManager} from "../TransferManager.sol";
import {TychoRouterV3} from "../TychoRouterV3.sol";

error TychoSubsidizingExecutor__InvalidDataLength();
error TychoSubsidizingExecutor__UnapprovedInnerExecutor(address executor);

/**
 * @title TychoSubsidizingExecutor
 * @notice Runs a hop through an inner executor the router approved, then asks
 *         the subsidy payer to pay a signed subsidy in the hop's output token
 *         to where the Dispatcher measures the hop's output.
 * @dev Swap data:
 *      [boundTo: 20][amount: 32][minAmountIn: 32][nonce: 32][deadline: 6]
 *      [signature: 65][innerExecutor: 20][innerData: ...]
 */
contract TychoSubsidizingExecutor is IExecutor {
    // The offsets below are used only as calldata slice bounds, which Slither
    // does not count as a use.
    // slither-disable-next-line unused-state
    uint256 private constant _BOUND_TO_OFFSET = 0;
    // slither-disable-next-line unused-state
    uint256 private constant _AMOUNT_OFFSET = 20;
    // slither-disable-next-line unused-state
    uint256 private constant _MIN_AMOUNT_IN_OFFSET = 52;
    // slither-disable-next-line unused-state
    uint256 private constant _NONCE_OFFSET = 84;
    // slither-disable-next-line unused-state
    uint256 private constant _DEADLINE_OFFSET = 116;
    // slither-disable-next-line unused-state
    uint256 private constant _SIGNATURE_OFFSET = 122;
    // slither-disable-next-line unused-state
    uint256 private constant _INNER_EXECUTOR_OFFSET = 187;
    uint256 private constant _INNER_DATA_OFFSET = 207;

    // Must equal Dispatcher._CURRENTLY_SWAPPING_EXECUTOR_SLOT, so the router's
    // fallback sends callbacks to the inner executor.
    uint256 internal constant _CURRENTLY_SWAPPING_EXECUTOR_SLOT =
        0x098a7a3b47801589e8cdf9ec791b93ad44273246946c32ef1fc4dbe45390c80e;

    ISubsidyPayer public immutable payer;
    address private immutable _self;

    event SubsidyPaymentFailed(uint256 nonce);

    constructor(ISubsidyPayer payer_) {
        payer = payer_;
        _self = address(this);
    }

    function swap(uint256 amountIn, bytes calldata data, address receiver)
        external
        payable
    {
        (IExecutor inner, bytes calldata innerData) =
            _inner(data, address(this));
        // slither-disable-next-line unused-return
        (,,, address tokenOut, bool outputToRouter) =
            inner.getTransferData(innerData);

        // slither-disable-next-line assembly
        assembly {
            tstore(_CURRENTLY_SWAPPING_EXECUTOR_SLOT, inner)
        }

        // The router approved the inner executor, and the Dispatcher sets
        // receiver.
        // slither-disable-next-line controlled-delegatecall,low-level-calls,missing-zero-check
        (bool success, bytes memory result) = address(inner)
            .delegatecall(
                abi.encodeCall(IExecutor.swap, (amountIn, innerData, receiver))
            );
        if (!success) {
            // slither-disable-next-line assembly
            assembly {
                revert(add(result, 0x20), mload(result))
            }
        }

        // The payer calls out to `to`; nothing may reach the inner executor
        // through the router's fallback from there.
        // slither-disable-next-line assembly
        assembly {
            tstore(_CURRENTLY_SWAPPING_EXECUTOR_SLOT, 0)
        }

        // Must equal the Dispatcher's measureAt in _callSwapOnExecutor, so the
        // Dispatcher counts the subsidy as hop output.
        address to = outputToRouter ? address(this) : receiver;
        _pay(data, tokenOut, amountIn, to);
    }

    function getTransferData(bytes calldata data)
        external
        view
        returns (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenIn,
            address tokenOut,
            bool outputToRouter
        )
    {
        (IExecutor inner, bytes calldata innerData) = _inner(data, msg.sender);
        (transferType, receiver, tokenIn, tokenOut, outputToRouter) =
            inner.getTransferData(innerData);
        receiver = _routerIfSelf(receiver);
    }

    function fundsExpectedAddress(bytes calldata data)
        external
        view
        returns (address receiver)
    {
        (IExecutor inner, bytes calldata innerData) = _inner(data, msg.sender);
        receiver = _routerIfSelf(inner.fundsExpectedAddress(innerData));
    }

    /// @dev A payer revert emits SubsidyPaymentFailed instead of reverting the
    ///      swap. The payer emits the events for paid and skipped subsidies.
    function _pay(
        bytes calldata data,
        address token,
        uint256 amountIn,
        address to
    ) internal {
        Subsidy memory subsidy = Subsidy({
            boundTo: address(bytes20(data[_BOUND_TO_OFFSET:_AMOUNT_OFFSET])),
            token: token,
            amount: uint256(
                bytes32(data[_AMOUNT_OFFSET:_MIN_AMOUNT_IN_OFFSET])
            ),
            minAmountIn: uint256(
                bytes32(data[_MIN_AMOUNT_IN_OFFSET:_NONCE_OFFSET])
            ),
            innerSwapHash: keccak256(data[_INNER_EXECUTOR_OFFSET:]),
            nonce: uint256(bytes32(data[_NONCE_OFFSET:_DEADLINE_OFFSET])),
            deadline: uint48(bytes6(data[_DEADLINE_OFFSET:_SIGNATURE_OFFSET]))
        });
        // msg.sender is the router's caller: the router delegatecalls swap.
        // slither-disable-next-line unused-return
        try payer.pay(
            subsidy,
            amountIn,
            msg.sender,
            to,
            data[_SIGNATURE_OFFSET:_INNER_EXECUTOR_OFFSET]
        ) {}
        catch {
            // Reentrancy cannot happen: every router entry point is
            // nonReentrant.
            // slither-disable-next-line reentrancy-events
            emit SubsidyPaymentFailed(subsidy.nonce);
        }
    }

    /// @dev The inner executor names its caller, this contract, to mean the
    ///      router.
    function _routerIfSelf(address receiver) internal view returns (address) {
        return receiver == address(this) ? msg.sender : receiver;
    }

    /// @dev Reverts unless `router` approved the inner executor and its
    ///      timelock passed.
    function _inner(bytes calldata data, address router)
        internal
        view
        returns (IExecutor inner, bytes calldata innerData)
    {
        if (data.length < _INNER_DATA_OFFSET) {
            revert TychoSubsidizingExecutor__InvalidDataLength();
        }
        address innerAddress =
            address(bytes20(data[_INNER_EXECUTOR_OFFSET:_INNER_DATA_OFFSET]));
        uint256 activation = TychoRouterV3(payable(router))
            .executorsActivationTimestamp(innerAddress);
        // slither-disable-next-line incorrect-equality
        if (activation == 0 || innerAddress == _self) {
            revert TychoSubsidizingExecutor__UnapprovedInnerExecutor(innerAddress);
        }
        // slither-disable-next-line timestamp
        if (block.timestamp < activation) {
            revert TychoSubsidizingExecutor__UnapprovedInnerExecutor(innerAddress);
        }
        inner = IExecutor(innerAddress);
        innerData = data[_INNER_DATA_OFFSET:];
    }
}
