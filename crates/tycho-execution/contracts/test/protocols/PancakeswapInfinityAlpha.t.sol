// SPDX-License-Identifier: LicenseRef-Fynd-License-1.1
pragma solidity ^0.8.26;

import "../TestUtils.sol";
import "../TychoRouterTestSetup.sol";

// USDT/KGEN on BSC, a CLAlphaHook v1 pool (hook 0x72e0…). Its start time was 2025-10-07 11:00
// UTC; at 11:55 the owner moved it to 14:00, pausing the live pool until then.
address constant BSC_USDT = 0x55d398326f99059fF775485246999027B3197955;
address constant BSC_KGEN = 0xF3d5b4c34Ed623478cc5141861776E6cf7AE3A1E;
address constant CLALPHA_HOOK_V1 = 0x72e09eBd9b24F47730b651889a4eD984CBa53d90;
// Bitmap 0x55 (beforeInitialize, beforeAdd/RemoveLiquidity, beforeSwap) and tick spacing 10.
bytes32 constant KGEN_POOL_PARAMETERS = bytes32(uint256(0x0a0055));
uint24 constant KGEN_POOL_FEE = 67;

// 14:49 UTC, after trading resumed.
uint256 constant ALPHA_OPEN_BLOCK = 63799000;
// 12:57 UTC, inside the pause.
uint256 constant ALPHA_PAUSED_BLOCK = 63790000;

function usdtKgenSwapData() pure returns (bytes memory) {
    return abi.encodePacked(
        BSC_USDT,
        BSC_KGEN,
        true, // USDT sorts below KGEN
        uint8(0), // CL
        KGEN_POOL_FEE,
        KGEN_POOL_PARAMETERS,
        CLALPHA_HOOK_V1
    );
}

// The Infinity executor needs nothing new for CLAlphaHook pools: the hook travels in the pool
// key and only gates on its start time.
abstract contract PancakeswapInfinityAlphaBase is TychoRouterTestSetup {
    function getChain() public pure override returns (string memory) {
        return "bsc";
    }

    function swapUsdtForKgen(uint256 amountIn) internal returns (uint256) {
        deal(BSC_USDT, ALICE, amountIn);
        vm.startPrank(ALICE);
        IERC20(BSC_USDT).approve(tychoRouterAddr, amountIn);
        uint256 out = tychoRouter.singleSwap(
            amountIn,
            BSC_USDT,
            BSC_KGEN,
            2 ** 255,
            1,
            ALICE,
            noClientFee(),
            encodeSingleSwap(
                address(pancakeswapInfinityExecutor), usdtKgenSwapData()
            )
        );
        vm.stopPrank();
        return out;
    }
}

contract PancakeswapInfinityAlphaOpenTest is PancakeswapInfinityAlphaBase {
    function getForkBlock() public pure override returns (uint256) {
        return ALPHA_OPEN_BLOCK;
    }

    function testSwapsThroughAnOpenAlphaPool() public {
        uint256 before = IERC20(BSC_KGEN).balanceOf(ALICE);

        uint256 out = swapUsdtForKgen(10e18);
        emit log_named_uint("KGEN out for 10 USDT", out);

        assertGt(out, 0);
        assertEq(IERC20(BSC_KGEN).balanceOf(ALICE) - before, out);
        assertEq(IERC20(BSC_USDT).balanceOf(tychoRouterAddr), 0);
        assertEq(IERC20(BSC_KGEN).balanceOf(tychoRouterAddr), 0);
    }
}

contract PancakeswapInfinityAlphaPausedTest is PancakeswapInfinityAlphaBase {
    function getForkBlock() public pure override returns (uint256) {
        return ALPHA_PAUSED_BLOCK;
    }

    /// The hook's `PoolNotStarted()` reverts the whole swap while the pool is paused.
    function testSwapRevertsWhileTheAlphaPoolIsPaused() public {
        deal(BSC_USDT, ALICE, 10e18);
        vm.startPrank(ALICE);
        IERC20(BSC_USDT).approve(tychoRouterAddr, 10e18);
        try tychoRouter.singleSwap(
            10e18,
            BSC_USDT,
            BSC_KGEN,
            2 ** 255,
            1,
            ALICE,
            noClientFee(),
            encodeSingleSwap(
                address(pancakeswapInfinityExecutor), usdtKgenSwapData()
            )
        ) {
            fail();
        } catch (bytes memory reason) {
            assertTrue(
                containsSelector(reason, bytes4(keccak256("PoolNotStarted()"))),
                "the revert must come from the hook's start-time gate"
            );
        }
        vm.stopPrank();
        assertEq(IERC20(BSC_USDT).balanceOf(ALICE), 10e18, "no funds moved");
    }

    function containsSelector(bytes memory data, bytes4 selector)
        internal
        pure
        returns (bool)
    {
        for (uint256 i = 0; i + 4 <= data.length; i++) {
            if (
                data[i] == selector[0] && data[i + 1] == selector[1]
                    && data[i + 2] == selector[2] && data[i + 3] == selector[3]
            ) {
                return true;
            }
        }
        return false;
    }
}
