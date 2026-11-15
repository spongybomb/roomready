import json
import re
import unittest
from pathlib import Path

from roomready import saga
from roomready.local_orchestrator import ASL_PATH, AslInterpreter, load_definition

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "infra" / "template.yaml"


class AslDefinitionTests(unittest.TestCase):
    def setUp(self):
        self.raw = json.loads(ASL_PATH.read_text(encoding="utf-8"))

    def test_is_valid_json_with_start_state(self):
        self.assertIn(self.raw["StartAt"], self.raw["States"])

    def test_every_task_maps_to_an_existing_step_handler(self):
        tasks = {n: s for n, s in self.raw["States"].items() if s["Type"] == "Task"}
        self.assertGreaterEqual(len(tasks), 10)
        used = set()
        for name, state in tasks.items():
            with self.subTest(state=name):
                self.assertEqual(state["Resource"], "${StepHandlerArn}")
                self.assertEqual(state["Parameters"]["state.$"], "$")
                self.assertIn(state["Parameters"]["step"], saga.STEP_HANDLERS)
                used.add(state["Parameters"]["step"])
        self.assertEqual(used, set(saga.STEP_HANDLERS))

    def test_all_transitions_target_existing_states(self):
        states = self.raw["States"]
        for name, state in states.items():
            targets = [state.get("Next"), state.get("Default")]
            targets += [c["Next"] for c in state.get("Choices", [])]
            targets += [c["Next"] for c in state.get("Catch", [])]
            for t in filter(None, targets):
                with self.subTest(state=name, target=t):
                    self.assertIn(t, states)
            if state["Type"] in ("Task", "Pass"):
                self.assertTrue(state.get("Next") or state.get("End"), name)

    def test_compensation_chain_is_reverse_of_forward(self):
        s = self.raw["States"]
        self.assertEqual(s["ReserveSupplies"]["Next"], "AfterReserveSupplies")
        self.assertEqual(s["AfterBookCourier"]["Default"], "SetupRoom")
        self.assertEqual(s["CompensateSetupRoom"]["Next"], "CompensateBookCourier")
        self.assertEqual(s["CompensateBookCourier"]["Next"], "CompensateReserveSupplies")
        self.assertEqual(s["CompensateReserveSupplies"]["Next"], "FinalizeCompensation")

    def test_only_substitution_is_step_handler_arn(self):
        text = ASL_PATH.read_text(encoding="utf-8")
        self.assertEqual(set(re.findall(r"\$\{(\w+)\}", text)), {"StepHandlerArn"})
        self.assertNotIn("${", json.dumps(load_definition()))


class InterpreterTests(unittest.TestCase):
    def test_retry_then_catch_with_result_path(self):
        definition = {"StartAt": "T", "States": {
            "T": {"Type": "Task", "Resource": "r", "Retry": [{"ErrorEquals": ["Boom"], "MaxAttempts": 2}],
                  "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "P"}], "Next": "S"},
            "P": {"Type": "Pass", "Result": "caught", "ResultPath": "$.note", "Next": "C"},
            "C": {"Type": "Choice", "Choices": [{"Variable": "$.note", "StringEquals": "caught", "Next": "S"}],
                  "Default": "F"},
            "S": {"Type": "Succeed"}, "F": {"Type": "Fail", "Error": "X"}}}
        calls = []

        class Boom(Exception):
            pass

        def invoke(resource, params):
            calls.append(params)
            raise Boom("nope")
        result = AslInterpreter(definition, invoke).run({"a": 1})
        self.assertEqual(len(calls), 3)
        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertEqual(result["output"]["error"]["Error"], "Boom")
        self.assertEqual(result["output"]["note"], "caught")


class TemplateTests(unittest.TestCase):
    def test_template_uses_only_baseline_services_and_no_iam_resources(self):
        text = TEMPLATE.read_text(encoding="utf-8")
        types = set(re.findall(r"Type:\s*(AWS::[A-Za-z0-9]+::[A-Za-z0-9]+)", text))
        allowed_prefixes = ("AWS::DynamoDB::", "AWS::Lambda::", "AWS::ApiGatewayV2::", "AWS::Events::",
                            "AWS::StepFunctions::")
        self.assertTrue(types)
        for t in types:
            self.assertTrue(t.startswith(allowed_prefixes), t)
        self.assertNotIn("AWS::IAM::", text)
        self.assertIn("!Ref LabRoleArn", text)
        self.assertIn("__STATE_MACHINE_DEFINITION__", text)
        self.assertIn("StateMachineType: STANDARD", text)
        self.assertIn("AuthorizationType: !Ref RouteAuthorization", text)

    def test_rendered_template_embeds_definition(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("render_template", ROOT / "scripts" / "render_template.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        rendered = mod.render()
        self.assertNotIn("__STATE_MACHINE_DEFINITION__", rendered)
        line = next(l for l in rendered.splitlines() if l.strip().startswith("Definition: {"))
        definition = json.loads(line.split("Definition:", 1)[1])
        self.assertEqual(definition, json.loads(ASL_PATH.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
