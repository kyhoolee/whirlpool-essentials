from typing import List, Optional, Tuple
from solders.account import Account
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
from ..types.types import BlockTimestamp
from .types import WhirlpoolsConfig, FeeTier, Whirlpool, TickArray, Position, PositionBundle, MintInfo, AccountInfo
from .account_parser import AccountParser
from .keyed_account_converter import KeyedAccountConverter
from dataclasses import replace

BULK_FETCH_CHUNK_SIZE = 100


# https://github.com/orca-so/whirlpools/blob/7b9ec351e2048c5504ffc8894c0ec5a9e78dc113/sdk/src/network/public/fetcher.ts
class AccountFetcher:
    def __init__(self, connection: AsyncClient):
        self._connection = connection
        self._cache = {}

    async def _get(self, pubkey: Pubkey, parser, keyed_converter, refresh: bool):
        key = str(pubkey)
        if not refresh and key in self._cache:
            return self._cache[key]

        res = await self._connection.get_account_info(pubkey, commitment="processed")
        if res.value is None:
            return None

        parsed = parser(res.value.data)
        # print(f'\n>>> PARSED {pubkey} {parsed}\n')
        if parsed is None:
            return None
        keyed = keyed_converter(pubkey, parsed)

        # print(f'\n>>> KEYED {pubkey} {keyed}\n')

        # keyed.slot = res.context.slot
        # Assuming `keyed` is a frozen dataclass object, recreate it with the new 'slot' value
        new_keyed = keyed
        if hasattr(keyed, 'slot'):
            new_keyed = replace(keyed, slot=res.context.slot)
        

        self._cache[key] = new_keyed
        return new_keyed

    async def _list(self, pubkeys: List[Pubkey], parser, keyed_converter, refresh: bool):
        fetch_needed = list(filter(lambda p: refresh or str(p) not in self._cache, pubkeys))

        if len(fetch_needed) > 0:
            fetched, slots = await self._bulk_fetch(fetch_needed)
            for i in range(len(fetch_needed)):
                if fetched[i] is None:
                    continue
                parsed = parser(fetched[i].data)
                if parsed is None:
                    continue
                keyed = keyed_converter(fetch_needed[i], parsed)

                new_keyed = keyed

                if hasattr(keyed, 'slot'):
                    new_keyed = replace(keyed, slot=slots[i])

                self._cache[str(fetch_needed[i])] = new_keyed

        return list(map(lambda p: self._cache.get(str(p)), pubkeys))

    async def _bulk_fetch(self, pubkeys: List[Pubkey]) -> Tuple[List[Optional[Account]], List[int]]:
        accounts = []
        slots = []
        for i in range(0, len(pubkeys), BULK_FETCH_CHUNK_SIZE):
            chunk = pubkeys[i:(i+BULK_FETCH_CHUNK_SIZE)]
            fetched = await self._connection.get_multiple_accounts(chunk, commitment="processed")
            slot = fetched.context.slot
            accounts.extend(fetched.value)
            slots.extend([slot] * len(chunk))
        return accounts, slots

    async def get_whirlpool(self, pubkey: Pubkey, refresh: bool = False) -> Optional[Whirlpool]:
        return await self._get(pubkey, AccountParser.parse_whirlpool, KeyedAccountConverter.to_keyed_whirlpool, refresh)

    async def get_whirlpools_config(self, pubkey: Pubkey, refresh: bool = False) -> Optional[WhirlpoolsConfig]:
        return await self._get(pubkey, AccountParser.parse_whirlpools_config, KeyedAccountConverter.to_keyed_whirlpools_config, refresh)

    async def get_fee_tier(self, pubkey: Pubkey, refresh: bool = False) -> Optional[FeeTier]:
        return await self._get(pubkey, AccountParser.parse_fee_tier, KeyedAccountConverter.to_keyed_fee_tier, refresh)

    async def get_position(self, pubkey: Pubkey, refresh: bool = False) -> Optional[Position]:
        return await self._get(pubkey, AccountParser.parse_position, KeyedAccountConverter.to_keyed_position, refresh)

    async def get_tick_array(self, pubkey: Pubkey, refresh: bool = False) -> Optional[TickArray]:
        return await self._get(pubkey, AccountParser.parse_tick_array, KeyedAccountConverter.to_keyed_tick_array, refresh)

    async def get_position_bundle(self, pubkey: Pubkey, refresh: bool = False) -> Optional[PositionBundle]:
        return await self._get(pubkey, AccountParser.parse_position_bundle, KeyedAccountConverter.to_keyed_position_bundle, refresh)

    async def get_token_account(self, pubkey: Pubkey, refresh: bool = False) -> Optional[AccountInfo]:
        return await self._get(pubkey, AccountParser.parse_token_account, KeyedAccountConverter.to_keyed_token_account, refresh)

    async def get_token_mint(self, pubkey: Pubkey, refresh: bool = False) -> Optional[MintInfo]:
        return await self._get(pubkey, AccountParser.parse_token_mint, KeyedAccountConverter.to_keyed_token_mint, refresh)

    async def list_whirlpools(self, pubkeys: List[Pubkey], refresh: bool = False) -> List[Optional[Whirlpool]]:
        return await self._list(pubkeys, AccountParser.parse_whirlpool, KeyedAccountConverter.to_keyed_whirlpool, refresh)

    async def list_positions(self, pubkeys: List[Pubkey], refresh: bool = False) -> List[Optional[Position]]:
        return await self._list(pubkeys, AccountParser.parse_position, KeyedAccountConverter.to_keyed_position, refresh)

    async def list_tick_arrays(self, pubkeys: List[Pubkey], refresh: bool = False) -> List[Optional[TickArray]]:
        return await self._list(pubkeys, AccountParser.parse_tick_array, KeyedAccountConverter.to_keyed_tick_array, refresh)

    async def list_position_bundles(self, pubkeys: List[Pubkey], refresh: bool = False) -> List[Optional[PositionBundle]]:
        return await self._list(pubkeys, AccountParser.parse_position_bundle, KeyedAccountConverter.to_keyed_position_bundle, refresh)

    async def list_token_accounts(self, pubkeys: List[Pubkey], refresh: bool = False) -> List[Optional[AccountInfo]]:
        return await self._list(pubkeys, AccountParser.parse_token_account, KeyedAccountConverter.to_keyed_token_account, refresh)

    async def list_token_mints(self, pubkeys: List[Pubkey], refresh: bool = False) -> List[Optional[MintInfo]]:
        return await self._list(pubkeys, AccountParser.parse_token_mint, KeyedAccountConverter.to_keyed_token_mint, refresh)

    async def get_latest_block_timestamp(self) -> BlockTimestamp:
        res1 = await self._connection.get_latest_blockhash()
        slot = res1.context.slot
        res2 = await self._connection.get_block_time(slot)
        timestamp = res2.value
        return BlockTimestamp(slot=slot, timestamp=timestamp)
