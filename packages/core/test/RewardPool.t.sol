pragma solidity 0.8.20;

import "forge-std/test.sol";
import "../src/Staking.sol";
import "../src/HMToken.sol";
import "../src/Escrow.sol";
import "../src/EscrowFactory.sol";
import "../src/RewardPool.sol";
import "./CoreUtils.t.sol";
import "../src/utils/SafeMath.sol";
import "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

interface RewardPoolEvents {
    event RewardAdded(address indexed escrowAddress, address indexed staker, address indexed slasher, uint256 tokens);
}

contract RewardPoolTest is CoreUtils, RewardPoolEvents {
    using SafeMath for uint256;

    function setUp() public {
        // Deploy HMTToken Contract
        vm.startPrank(owner);
        hmToken = new HMToken(1000000000, 'Human Token', 18, 'HMT');
        // Deploy Staking Contract
        address stakingImpl = address(new Staking());
        bytes memory stakingData =
            abi.encodeWithSelector(Staking.initialize.selector, address(hmToken), minimumStake, lockPeriod);
        address stakingProxy = address(new ERC1967Proxy(stakingImpl, stakingData));
        staking = Staking(stakingProxy);
        //Deploy Escrow Factory proxy
        address escrowFactoryImpl = address(new EscrowFactory());
        bytes memory escrowFactoryData = abi.encodeWithSelector(EscrowFactory.initialize.selector, address(staking));
        address escrowFactoryProxy = address(new ERC1967Proxy(escrowFactoryImpl, escrowFactoryData));
        escrowFactory = EscrowFactory(escrowFactoryProxy);
        vm.stopPrank();

        // Send HMT tokens to contract participants
        vm.startPrank(owner);
        for (uint256 i = 0; i < accounts.length - 1; i++) {
            hmToken.approve(accounts[i + 1], 1000);
        }
        vm.stopPrank();
        for (uint256 i = 0; i < accounts.length - 1; i++) {
            vm.prank(accounts[i + 1]);
            hmToken.transferFrom(owner, accounts[i], 1000);
        }

        vm.startPrank(owner);
        // Deploy Reward Pool
        address rewardPoolImpl = address(new RewardPool());
        bytes memory rewardPoolData =
            abi.encodeWithSelector(RewardPool.initialize.selector, address(hmToken), address(staking), rewardFee);
        address rewardPoolProxy = address(new ERC1967Proxy(rewardPoolImpl, rewardPoolData));
        rewardPool = RewardPool(rewardPoolProxy);

        // Configure Reward Pool in Staking
        staking.setRewardPool(address(rewardPool));
        vm.stopPrank();

        // Approve spend HMT tokens staking contract
        for (uint256 i = 0; i < accounts.length - 1; i++) {
            vm.prank(accounts[i + 1]);
            hmToken.approve(address(staking), 1000);
        }
    }

    function testSetTokenAddress() public {
        assertEq(rewardPool.token(), address(hmToken), "Token address is not set correctly");
    }

    function testSetFee() public {
        assertEq(rewardPool.fees(), 2, "Fee is not set correctly");
    }

    function testFail_OnlyStakingCanAddReward() public {
        uint256 stakedTokens = 10;
        uint256 allocatedTokens = 5;

        vm.prank(validator);
        staking.stake(stakedTokens);
        vm.startPrank(operator);
        staking.stake(stakedTokens);
        // Create a dynamic array of addresses with one element
        address[] memory trustedHandlers = new address[](1);
        trustedHandlers[0] = validator;
        address escrowAddress = escrowFactory.createEscrow(address(hmToken), trustedHandlers, jobRequesterId);
        staking.allocate(escrowAddress, allocatedTokens);
        vm.expectRevert("CALLER_IS_NOT_STAKING_CONTRACT");
        rewardPool.addReward(address(0), address(0), address(0), 1);
        vm.stopPrank();
    }

    function testRewardNotCreatedWhenSlashedHigherThanFee() public {
        uint256 stakedTokens = 10;
        uint256 allocatedTokens = 5;

        vm.prank(validator);
        staking.stake(stakedTokens);
        vm.startPrank(operator);
        staking.stake(stakedTokens);
        // Create a dynamic array of addresses with one element
        address[] memory trustedHandlers = new address[](1);
        trustedHandlers[0] = validator;
        address escrowAddress = escrowFactory.createEscrow(address(hmToken), trustedHandlers, jobRequesterId);
        staking.allocate(escrowAddress, allocatedTokens);
        uint256 slashedTokens = 1;
        vm.prank(owner);
        staking.slash(validator, operator, escrowAddress, slashedTokens);
        assertEq(hmToken.balanceOf(address(rewardPool)), slashedTokens, "Reward is not created correctly");
        IRewardPool.Reward[] memory rewards = rewardPool.getRewards(escrowAddress);
        assertEq(rewards.length, 0);
    }

    function testRewardCreatedWhenSlashedHigherThanFee() public {
        uint256 stakedTokens = 10;
        uint256 allocatedTokens = 5;

        vm.prank(validator);
        staking.stake(stakedTokens);
        vm.startPrank(operator);
        staking.stake(stakedTokens);
        // Create a dynamic array of addresses with one element
        address[] memory trustedHandlers = new address[](1);
        trustedHandlers[0] = validator;
        address escrowAddress = escrowFactory.createEscrow(address(hmToken), trustedHandlers, jobRequesterId);
        staking.allocate(escrowAddress, allocatedTokens);
        uint256 slashedTokens = 3;
        vm.prank(owner);
        vm.expectEmit();
        emit RewardAdded(escrowAddress, operator, validator, slashedTokens - rewardFee);
        staking.slash(validator, operator, escrowAddress, slashedTokens);
        assertEq(hmToken.balanceOf(address(rewardPool)), slashedTokens);
        IRewardPool.Reward[] memory rewards = rewardPool.getRewards(escrowAddress);
        assertEq(rewards.length, 1);
        assertEq(rewards[0].escrowAddress, escrowAddress);
        assertEq(rewards[0].slasher, validator);
        assertEq(rewards[0].tokens, slashedTokens - rewardFee);
    }

    function testFail_RevertWhenNoReward() public {
        uint256 stakedTokens = 10;
        uint256 allocatedTokens = 8;

        // Init validator
        address[] memory trustedHandlers = new address[](1);
        trustedHandlers[0] = validator;
        vm.prank(validator);
        staking.stake(stakedTokens);
        vm.prank(validator2);
        staking.stake(stakedTokens);
        vm.startPrank(operator);
        staking.stake(stakedTokens);

        address escrowAddress = escrowFactory.createEscrow(address(hmToken), trustedHandlers, jobRequesterId);
        staking.allocate(escrowAddress, allocatedTokens);

        vm.expectRevert("NO_REWARDS_FOR_ESCROW");
        rewardPool.distributeReward(escrowAddress);
        vm.stopPrank();
    }

    function testDistributeReward() public {
        uint256 stakedTokens = 10;
        uint256 allocatedTokens = 8;
        uint256 vSlashAmount = 2;
        uint256 v2SlashAmount = 3;

        // Init validator
        address[] memory trustedHandlers = new address[](1);
        trustedHandlers[0] = validator;
        vm.prank(validator);
        staking.stake(stakedTokens);
        vm.prank(validator2);
        staking.stake(stakedTokens);
        vm.startPrank(operator);
        staking.stake(stakedTokens);
        address escrowAddress = escrowFactory.createEscrow(address(hmToken), trustedHandlers, jobRequesterId);
        staking.allocate(escrowAddress, allocatedTokens);
        vm.stopPrank();

        vm.startPrank(owner);
        staking.slash(validator, operator, escrowAddress, vSlashAmount);
        staking.slash(validator2, operator, escrowAddress, v2SlashAmount);

        uint256 vBalanceBefore = hmToken.balanceOf(validator);
        uint256 v2BalanceBefore = hmToken.balanceOf(validator2);
        rewardPool.distributeReward(escrowAddress);
        assertEq(hmToken.balanceOf(validator), vBalanceBefore + (vSlashAmount - rewardFee));
        assertEq(hmToken.balanceOf(validator2), v2BalanceBefore + (v2SlashAmount - rewardFee));
        assertEq(hmToken.balanceOf(address(rewardPool)), rewardFee.mul(2));
    }

    function testWithdrawReward() public {
        uint256 stakedTokens = 10;
        uint256 allocatedTokens = 8;

        // Init validator
        address[] memory trustedHandlers = new address[](1);
        trustedHandlers[0] = validator;
        vm.prank(validator);
        staking.stake(stakedTokens);
        vm.prank(validator2);
        staking.stake(stakedTokens);
        vm.startPrank(operator);
        staking.stake(stakedTokens);
        address escrowAddress = escrowFactory.createEscrow(address(hmToken), trustedHandlers, jobRequesterId);
        staking.allocate(escrowAddress, allocatedTokens);
        vm.stopPrank();

        vm.startPrank(owner);
        uint256 oBalanceBefore = hmToken.balanceOf(owner);
        rewardPool.withdraw(owner);
        assertEq(hmToken.balanceOf(owner) / (10 ** 25), (oBalanceBefore + rewardFee.mul(2)) / (10 ** 25));
    }
}