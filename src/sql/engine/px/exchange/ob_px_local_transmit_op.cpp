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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_px_local_transmit_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObPxLocalTransmitOpInput, ObPxTransmitOpInput));

OB_SERIALIZE_MEMBER((ObPxLocalTransmitSpec, ObPxTransmitSpec));

ObPxLocalTransmitOp::ObPxLocalTransmitOp(
  ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
: ObPxTransmitOp(exec_ctx, spec, input)
{
}

int ObPxLocalTransmitOp::inner_open()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("Inner open px fifo transmit", "op_id", MY_SPEC.id_);
  if (OB_FAIL(ObPxTransmitOp::inner_open())) {
    LOG_WARN("initialize operator context failed", K(ret));
  }
  return ret;
}

void ObPxLocalTransmitOp::destroy()
{ 
  ObPxTransmitOp::destroy();
}

int ObPxLocalTransmitOp::do_transmit()
{
  int ret = OB_SUCCESS;
  ObLocalRandomSliceIdxCalc local_random_slice_id_calc(ctx_.get_allocator(), task_channels_);
  if (OB_FAIL(local_random_slice_id_calc.init())) {
    LOG_WARN("failed to init repart slice calc", K(ret));
  } else if (OB_FAIL(send_rows(local_random_slice_id_calc))) {
    LOG_WARN("local shuffle failed", K(ret));
  }
  return ret;
}

int ObPxLocalTransmitOp::inner_close()
{
  return ObPxTransmitOp::inner_close();
}

} // end namespace sql
} // end namespace oceanbase
