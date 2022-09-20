import { EnhancedEvent, OnChainData, processOnChainData } from "@/events-sync/handlers/utils";

import * as zora from "@/exchanges/zora/handler";
import * as cryptopunks from "@/events-sync/handlers/cryptopunks";
import * as foundation from "@/events-sync/handlers/foundation";
import * as nftx from "@/events-sync/handlers/nftx";
import * as nouns from "@/events-sync/handlers/nouns";
import * as sudoswap from "@/events-sync/handlers/sudoswap";
import * as wyvern from "@/events-sync/handlers/wyvern";

export type EventsInfo = {
  kind: "cryptopunks" | "foundation" | "nftx" | "nouns" | "sudoswap" | "wyvern" | "zora";
  events: EnhancedEvent[];
  backfill?: boolean;
};

export const processEvents = async (info: EventsInfo) => {
  let data: OnChainData | undefined;
  switch (info.kind) {
    case "zora": {
      data = await zora.handleEvents(info.events);
      break;
    }

    case "cryptopunks": {
      data = await cryptopunks.handleEvents(info.events);
      break;
    }

    case "foundation": {
      data = await foundation.handleEvents(info.events);
      break;
    }

    case "nftx": {
      data = await nftx.handleEvents(info.events);
      break;
    }

    case "nouns": {
      data = await nouns.handleEvents(info.events);
      break;
    }

    case "sudoswap": {
      data = await sudoswap.handleEvents(info.events);
      break;
    }

    case "wyvern": {
      data = await wyvern.handleEvents(info.events);
      break;
    }
  }

  if (data) {
    await processOnChainData(data, info.backfill);
  }
};
