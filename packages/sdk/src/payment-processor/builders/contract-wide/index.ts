import { BigNumberish } from "@ethersproject/bignumber";

import { BaseBuildParams, BaseBuilder } from "../base";
import { Order } from "../../order";
import { s } from "../../../utils";
import { AddressZero } from "@ethersproject/constants";

interface BuildParams extends BaseBuildParams {}

export class ContractWideBuilder extends BaseBuilder {
  public isValid(order: Order): boolean {
    try {
      const copyOrder = this.build({
        ...order.params,
      });

      if (!copyOrder) {
        return false;
      }

      if (copyOrder.hash() !== order.hash()) {
        return false;
      }
    } catch {
      return false;
    }

    return true;
  }

  public build(params: BuildParams) {
    if (params.sellerAcceptedOffer != undefined) {
      throw new Error("Unsupported order side");
    }

    this.defaultInitialize(params);
    return new Order(this.chainId, {
      kind: "collection-offer-approval",
      protocol: params.protocol,
      collectionLevelOffer: params.collectionLevelOffer,
      marketplace: params.marketplace!,
      marketplaceFeeNumerator: s(params.marketplaceFeeNumerator),
      maxRoyaltyFeeNumerator: s(params.maxRoyaltyFeeNumerator),
      privateTaker: params.privateTaker ?? AddressZero,
      trader: params.trader,
      tokenAddress: params.tokenAddress,
      amount: s(params.amount),
      price: s(params.price),
      expiration: s(params.expiration),
      nonce: s(params.nonce),
      coin: params.coin,
      masterNonce: s(params.masterNonce),
      v: params.v,
      r: params.r,
      s: params.s,
    });
  }

  public buildMatching(
    order: Order,
    options: {
      taker: string;
      takerNonce: BigNumberish;
      tokenId?: BigNumberish;
    }
  ): Order {
    const orderParams = order.params;
    return new Order(order.chainId, {
      protocol: orderParams.protocol,
      sellerAcceptedOffer: false,
      marketplace: orderParams.marketplace,
      marketplaceFeeNumerator: orderParams.marketplaceFeeNumerator,
      maxRoyaltyFeeNumerator: orderParams.maxRoyaltyFeeNumerator,
      privateTaker: orderParams.privateTaker,
      trader: options.taker,
      tokenAddress: orderParams.tokenAddress,
      tokenId: s(options.tokenId),
      amount: orderParams.amount,
      price: orderParams.price,
      expiration: orderParams.expiration,
      nonce: orderParams.nonce,
      coin: orderParams.coin,
      masterNonce: s(options.takerNonce),
    });
  }
}
