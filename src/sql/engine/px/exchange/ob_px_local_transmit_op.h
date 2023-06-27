/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_LOCAL_TRANSMIT_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_LOCAL_TRANSMIT_OP_H_

#include "ob_px_transmit_op.h"

namespace oceanbase
{
namespace sql
{

class ObPxLocalTransmitOpInput : public ObPxTransmitOpInput
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObPxLocalTransmitOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxTransmitOpInput(ctx, spec)
  {}
  virtual ~ObPxLocalTransmitOpInput()
  {}
};

class ObPxLocalTransmitSpec : public ObPxTransmitSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxLocalTransmitSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObPxTransmitSpec(alloc, type)
  {}
  ~ObPxLocalTransmitSpec() {}
};

class ObPxLocalTransmitOp : public ObPxTransmitOp
{
public:
  ObPxLocalTransmitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObPxLocalTransmitOp() { destroy(); }

  virtual int inner_open() override;
  virtual void destroy() override;
  virtual int inner_rescan() override { return ObPxTransmitOp::inner_rescan(); }
  virtual int inner_close() override;
  int do_transmit();
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_LOCAL_TRANSMIT_OP_H_
