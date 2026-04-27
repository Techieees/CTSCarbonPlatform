(function () {
  const form = document.getElementById("awardsBuilderForm");
  const list = document.getElementById("awardsQuestionsList");
  const addButton = document.getElementById("awardsAddQuestion");
  const payloadInput = document.getElementById("awardsQuestionsPayload");
  const initialNode = document.getElementById("awards-builder-initial");

  if (!form || !list || !addButton || !payloadInput || !initialNode) {
    return;
  }

  let questions = [];

  try {
    const parsed = JSON.parse(initialNode.textContent || "[]");
    questions = Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    questions = [];
  }

  function normalizeQuestion(question) {
    const type = String(question && question.question_type || "text").trim() || "text";
    const options = Array.isArray(question && question.options) ? question.options : [];
    return {
      question_text: String(question && question.question_text || "").trim(),
      question_type: type,
      required: Boolean(question && question.required),
      options: options.map(function (item) {
        return String(item || "").trim();
      }).filter(Boolean)
    };
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function syncPayload() {
    payloadInput.value = JSON.stringify(questions.map(normalizeQuestion));
  }

  function questionCard(question, index) {
    const wrapper = document.createElement("section");
    wrapper.className = "border rounded-3 p-3";
    wrapper.dataset.questionIndex = String(index);
    wrapper.innerHTML = '' +
      '<div class="d-flex align-items-start justify-content-between gap-2 flex-wrap mb-3">' +
        '<strong>Question ' + (index + 1) + '</strong>' +
        '<div class="d-flex gap-2 flex-wrap">' +
          '<button type="button" class="btn btn-outline-secondary btn-sm" data-action="move-up">Up</button>' +
          '<button type="button" class="btn btn-outline-secondary btn-sm" data-action="move-down">Down</button>' +
          '<button type="button" class="btn btn-outline-danger btn-sm" data-action="remove">Remove</button>' +
        '</div>' +
      '</div>' +
      '<div class="d-flex flex-column gap-3">' +
        '<div>' +
          '<label class="form-label">Question text</label>' +
          '<input type="text" class="form-control" data-field="question_text" value="' + escapeHtml(question.question_text || "") + '">' +
        '</div>' +
        '<div>' +
          '<label class="form-label">Question type</label>' +
          '<select class="form-select" data-field="question_type">' +
            '<option value="text"' + (question.question_type === "text" ? " selected" : "") + '>Single line text</option>' +
            '<option value="textarea"' + (question.question_type === "textarea" ? " selected" : "") + '>Multi-line text</option>' +
            '<option value="single_choice"' + (question.question_type === "single_choice" ? " selected" : "") + '>Single choice</option>' +
            '<option value="file"' + (question.question_type === "file" ? " selected" : "") + '>File upload</option>' +
          '</select>' +
        '</div>' +
        '<div class="form-check">' +
          '<input class="form-check-input" type="checkbox" data-field="required" id="awardsRequired' + index + '"' + (question.required ? " checked" : "") + '>' +
          '<label class="form-check-label" for="awardsRequired' + index + '">Required</label>' +
        '</div>' +
        '<div data-options-wrap' + (question.question_type === "single_choice" ? "" : ' hidden') + '>' +
          '<label class="form-label">Options</label>' +
          '<textarea class="form-control" rows="4" data-field="options">' + escapeHtml((question.options || []).join("\n")) + '</textarea>' +
          '<div class="form-text">One option per line.</div>' +
        '</div>' +
      '</div>';
    return wrapper;
  }

  function render() {
    list.innerHTML = "";
    questions.forEach(function (question, index) {
      list.appendChild(questionCard(question, index));
    });
    syncPayload();
  }

  function updateQuestion(index, field, value, rerender) {
    if (!questions[index]) {
      return;
    }
    if (field === "required") {
      questions[index][field] = Boolean(value);
    } else if (field === "options") {
      questions[index][field] = String(value || "").split("\n").map(function (item) {
        return item.trim();
      }).filter(Boolean);
    } else {
      questions[index][field] = value;
    }
    if (rerender) {
      render();
      return;
    }
    syncPayload();
  }

  addButton.addEventListener("click", function () {
    questions.push({
      question_text: "",
      question_type: "text",
      required: false,
      options: []
    });
    render();
  });

  list.addEventListener("input", function (event) {
    const card = event.target.closest("[data-question-index]");
    const field = event.target.getAttribute("data-field");
    if (!card || !field) {
      return;
    }
    const index = Number(card.dataset.questionIndex || -1);
    updateQuestion(index, field, event.target.value, false);
  });

  list.addEventListener("change", function (event) {
    const card = event.target.closest("[data-question-index]");
    const field = event.target.getAttribute("data-field");
    if (!card || !field) {
      return;
    }
    const index = Number(card.dataset.questionIndex || -1);
    if (field === "required") {
      updateQuestion(index, field, event.target.checked, false);
      return;
    }
    updateQuestion(index, field, event.target.value, field === "question_type");
  });

  list.addEventListener("click", function (event) {
    const button = event.target.closest("[data-action]");
    if (!button) {
      return;
    }
    const card = button.closest("[data-question-index]");
    const index = Number(card && card.dataset.questionIndex || -1);
    if (index < 0 || !questions[index]) {
      return;
    }
    const action = button.getAttribute("data-action");
    if (action === "remove") {
      questions.splice(index, 1);
    } else if (action === "move-up" && index > 0) {
      const item = questions.splice(index, 1)[0];
      questions.splice(index - 1, 0, item);
    } else if (action === "move-down" && index < questions.length - 1) {
      const item = questions.splice(index, 1)[0];
      questions.splice(index + 1, 0, item);
    }
    render();
  });

  form.addEventListener("submit", function () {
    syncPayload();
  });

  render();
})();
